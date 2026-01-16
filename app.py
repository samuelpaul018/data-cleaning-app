import io
import zipfile
import calendar
from datetime import datetime
import pandas as pd
import streamlit as st
from pandas.tseries.offsets import MonthEnd

# =============================
# Page setup
# =============================
st.set_page_config(
    page_title="Residuals Pipeline - Step 1",
    page_icon="🧹",
    layout="wide",
)

st.title("🧹 Residuals Data Cleaning Pipeline (Full Sync)")

# =============================
# Sidebar: Settings
# =============================
with st.sidebar:
    st.header("⚙️ Settings")
    col1, col2 = st.columns(2)
    with col1:
        month = st.selectbox("Month", list(range(1, 13)), index=datetime.now().month - 1,
                             format_func=lambda m: datetime(2000, m, 1).strftime("%B"))
    with col2:
        year = st.selectbox("Year", list(range(2020, 2031)), index=(datetime.now().year - 2020))

    last_day = calendar.monthrange(year, month)[1]
    selected_month_year = pd.Timestamp(year=year, month=month, day=last_day)
    six_months_before = selected_month_year - pd.DateOffset(months=6) + MonthEnd(0)

# =============================
# Notebook-Exact Helpers
# =============================
def clean_nbsp(df):
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str).str.replace(r"[\xa0Â]", "", regex=True).str.strip()
    return df

def clean_id(s):
    s = s.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return s

# =============================
# Core Pipeline Logic
# =============================

def run_pipeline(files, selected_month_year, six_months_before):
    # 1. Load Synoptics
    tsys = clean_nbsp(pd.read_csv(files["Synoptic_TSYS"]))
    fiserv = clean_nbsp(pd.read_csv(files["Synoptic_Fiserv"], skiprows=1))
    
    # TSYS Filtering
    tsys["Date Opened"] = pd.to_datetime(tsys["Date Opened"], errors='coerce')
    tsys["Date Closed"] = pd.to_datetime(tsys["Date Closed"], errors='coerce')
    tsys["Last Deposit Date"] = pd.to_datetime(tsys["Last Deposit Date"], errors='coerce')
    tsys = tsys[~(tsys["Date Opened"] > selected_month_year)]
    tsys.loc[(tsys["Status"].str.lower() == "closed") & (tsys["Date Closed"] > selected_month_year), "Status"] = "Open"
    tsys = tsys[~((tsys["Status"].str.lower() == "closed") & (tsys["Last Deposit Date"].isna() | (tsys["Last Deposit Date"] <= six_months_before)))]
    tsys = tsys[~tsys["Status"].str.lower().isin(["closed", "declined", "cancelled"])]
    tsys = tsys[~tsys["Rep Name"].str.lower().isin(["hubwallet", "stephany perez", "nigel westbury", "brandon_casillas"])]

    # Fiserv Filtering
    fiserv["Open Date"] = pd.to_datetime(fiserv["Open Date"], errors='coerce')
    fiserv = fiserv[~(fiserv["Open Date"] > selected_month_year)]
    fiserv = fiserv[fiserv["Sales Agent"].str.contains(r"[A-Za-z]", na=False) | fiserv["Sales Agent"].isin(["2030", "3030", "4030", "5030"])]
    fiserv = fiserv[fiserv["Sales Agent"] != "IS02"]

    # 2. PASO Output
    p1 = pd.read_csv(files["PASO_S1"], skiprows=1)
    p2 = pd.read_csv(files["PASO_S2"], skiprows=1)
    paso_all = pd.concat([p1, p2])
    paso_output = paso_all[paso_all["MerchantNumber"].astype(str).isin(fiserv["Merchant #"].astype(str))]

    # 3. Zoho & Fees
    zoho = pd.read_excel(files["Zoho_All_Fees"], skiprows=6)
    zoho = clean_nbsp(zoho)
    zoho["Merchant Number"] = clean_id(zoho["Merchant Number"])
    zoho = zoho[zoho["Sales Id"].str.contains(r"[A-Za-z]", na=False)]
    zoho = zoho[~zoho["Account Status"].str.lower().isin(["closed", "declined", "n/a", ""])]
    
    # Step 1 Mapping from MEX
    mex = pd.read_excel(files["MEX_file"])
    mex_cols = ["visa_base_rate_discount_rev", "mc_base_rate_discount_rev", "disc_base_rate_discount_rev", "amex_base_rate_discount_rev"]
    mex["step1_calc"] = mex[mex_cols].sum(axis=1)
    mex_lookup = mex.groupby("merchant_id")["step1_calc"].sum().to_dict()
    zoho["Step 1"] = zoho["Merchant Number"].map(lambda x: mex_lookup.get(int(x) if x.isdigit() else x, 0))

    # 4. Wireless Count Logic
    wireless_raw = pd.read_excel(files["Zoho_Wireless"], skiprows=6)
    WCV = clean_nbsp(wireless_raw)
    # Extracts Merchant Number and Wireless Count (matching notebook logic)
    WCV.rename(columns={WCV.columns[0]: "Mer_Wir", WCV.columns[5]: "MID"}, inplace=True)
    WCV["Wireless Count"] = WCV["Mer_Wir"].str.extract(r"\((\d+)\)")
    wireless_final = WCV[["MID", "Wireless Count"]].dropna().drop_duplicates(subset=["MID"])

    # 5. Valor ISO Report
    valor = pd.read_excel(files["Valor"])
    valor = clean_nbsp(valor)
    valor["MID1"] = clean_id(valor["MID1"])
    
    # Prefix TSYS with 39 if missing
    tsys_mask = valor["PROCESSOR"].str.lower().str.contains("tsys", na=False)
    valor.loc[tsys_mask & ~valor["MID1"].str.startswith("39"), "MID1"] = "39" + valor["MID1"]
    
    # Map Wireless Count into Valor
    wireless_dict = wireless_final.set_index("MID")["Wireless Count"].to_dict()
    valor["Wireless count"] = valor["MID1"].map(wireless_dict)
    valor[" "] = "" # Extra blank column for parity

    # =============================
    # Prepare Outputs
    # =============================
    outputs = {}
    outputs["PASO_Output.csv"] = paso_output.to_csv(index=False).encode('utf-8')
    outputs["MEX_Output.csv"] = mex.to_csv(index=False).encode('utf-8')
    
    # Monthly Min Excel
    min_buf = io.BytesIO()
    with pd.ExcelWriter(min_buf, engine='openpyxl') as writer:
        zoho[zoho["Processor"].str.lower() == "fiserv"].to_excel(writer, sheet_name="Fiserv", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Step1", index=False)
        zoho[zoho["Processor"].str.lower() == "tsys"].to_excel(writer, sheet_name="TSYS", index=False)
        mex.to_excel(writer, sheet_name="MEX", index=False)
    outputs["Monthly_min_and_annual_PCI_without_Step1_Output.xlsx"] = min_buf.getvalue()

    # Valor Excel
    valor_buf = io.BytesIO()
    with pd.ExcelWriter(valor_buf, engine='openpyxl') as writer:
        valor.to_excel(writer, sheet_name="ISO Report", index=False)
        wireless_final.to_excel(writer, sheet_name="Wireless Count", index=False)
    outputs["Valor_1ST_level_Output.xlsx"] = valor_buf.getvalue()

    return outputs

# =============================
# UI Components
# =============================
st.header("1. Upload Inputs")
col1, col2 = st.columns(2)
with col1:
    f_tsys = st.file_uploader("Synoptic TSYS", type="csv")
    f_fiserv = st.file_uploader("Synoptic Fiserv", type="csv")
    f_zoho = st.file_uploader("Zoho Fees", type="xlsx")
    f_wireless = st.file_uploader("Zoho Wireless", type="xlsx")
with col2:
    f_mex = st.file_uploader("MEX File", type="xlsx")
    f_s1 = st.file_uploader("PASO S1", type="csv")
    f_s2 = st.file_uploader("PASO S2", type="csv")
    f_valor = st.file_uploader("Valor", type="xlsx")

if all([f_tsys, f_fiserv, f_zoho, f_mex, f_s1, f_s2, f_wireless, f_valor]):
    if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
        files = {
            "Synoptic_TSYS": f_tsys, "Synoptic_Fiserv": f_fiserv,
            "Zoho_All_Fees": f_zoho, "MEX_file": f_mex,
            "PASO_S1": f_s1, "PASO_S2": f_s2,
            "Zoho_Wireless": f_wireless, "Valor": f_valor
        }
        results = run_pipeline(files, selected_month_year, six_months_before)
        
        st.success("✅ All 4 Files Generated Successfully!")
        
        # Individual Downloads
        for name, data in results.items():
            st.download_button(f"⬇️ Download {name}", data, file_name=name, use_container_width=True)
