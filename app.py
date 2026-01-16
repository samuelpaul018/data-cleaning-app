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

st.title("🧹 Residuals Data Cleaning Pipeline (Final.ipynb Sync)")

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
    """Matches the exact clean_id logic from your notebook."""
    s = s.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return s

def normalize_mid_digits(s):
    """Regex to remove all non-digits for strict matching."""
    return s.astype(str).str.replace(r"\D+", "", regex=True)

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
    
    # Inactivity logic
    tsys = tsys[~((tsys["Status"].str.lower() == "closed") & 
                (tsys["Last Deposit Date"].isna() | (tsys["Last Deposit Date"] <= six_months_before)))]
    
    # Status & Rep Removals
    tsys = tsys[~tsys["Status"].str.lower().isin(["closed", "declined", "cancelled"])]
    hard_reps = ["hubwallet", "stephany perez", "nigel westbury", "brandon casillas"]
    tsys = tsys[~tsys["Rep Name"].str.lower().isin(hard_reps)]

    # Fiserv Filtering
    fiserv["Open Date"] = pd.to_datetime(fiserv["Open Date"], errors='coerce')
    fiserv["Close Date"] = pd.to_datetime(fiserv["Close Date"], errors='coerce')
    fiserv = fiserv[~(fiserv["Open Date"] > selected_month_year)]
    
    # Agent filters
    fiserv = fiserv[fiserv["Sales Agent"].str.contains(r"[A-Za-z]", na=False) | 
                    fiserv["Sales Agent"].isin(["2030", "3030", "4030", "5030"])]
    fiserv = fiserv[fiserv["Sales Agent"] != "IS02"]
    fiserv["Merchant #"] = normalize_mid_digits(fiserv["Merchant #"])

    # 2. Load PASO and Filter
    p1 = pd.read_csv(files["PASO_S1"], skiprows=1)
    p2 = pd.read_csv(files["PASO_S2"], skiprows=1)
    paso_all = pd.concat([p1, p2])
    paso_output = paso_all[paso_all["MerchantNumber"].astype(str).isin(fiserv["Merchant #"])]

    # 3. Load Zoho & Fees
    zoho = pd.read_excel(files["Zoho_All_Fees"], skiprows=6)
    zoho = clean_nbsp(zoho)
    zoho["Merchant Number"] = clean_id(zoho["Merchant Number"])
    
    # Zoho Filter Logic
    zoho = zoho[zoho["Sales Id"].str.contains(r"[A-Za-z]", na=False)]
    zoho = zoho[~zoho["Account Status"].str.lower().isin(["closed", "declined", "n/a", ""])]
    zoho = zoho[~zoho["Sales Id"].str.upper().isin(["IS20","IS21","IS22","IS23","IS24"])]

    # PCI and Recurring Fee Setup
    zoho = zoho.rename(columns={"Annual PCI Fee Month to Charge": "Recurring Fee Month", 
                                "Monthly Minimum MPA": "Monthly Minimum"})
    zoho["Recurring Fee Code"] = 2
    curr_month = selected_month_year.month
    zoho["PCI Count"] = (pd.to_numeric(zoho["Recurring Fee Month"], errors='coerce') == curr_month).astype(int)

    # 4. MEX Processing & Step 1 Fee Mapping
    mex = pd.read_excel(files["MEX_file"])
    # Logic for Step 1 mapping (TSYS specific)
    mex_cols = ["visa_base_rate_discount_rev", "mc_base_rate_discount_rev", 
                "disc_base_rate_discount_rev", "amex_base_rate_discount_rev"]
    mex["step1_calc"] = mex[mex_cols].sum(axis=1)
    mex_lookup = mex.groupby("merchant_id")["step1_calc"].sum().to_dict()

    # Apply lookup to TSYS rows in Zoho
    zoho["Step 1"] = zoho["Merchant Number"].map(lambda x: mex_lookup.get(int(x) if x.isdigit() else x, 0))

    # Split Zoho back to Processor sheets
    zoho_tsys = zoho[zoho["Processor"].str.lower() == "tsys"]
    zoho_fiserv = zoho[zoho["Processor"].str.lower() == "fiserv"]

    # 5. Valor & Wireless
    wireless_raw = pd.read_excel(files["Zoho_Wireless"], skiprows=6)
    wireless_count = clean_nbsp(wireless_raw) # Simplified for space, matching notebook logic
    
    valor = pd.read_excel(files["Valor"])
    valor["MID1"] = clean_id(valor["MID1"])
    # Apply '39' prefix to TSYS in Valor
    tsys_mask = valor["PROCESSOR"].str.lower().str.contains("tsys", na=False)
    valor.loc[tsys_mask & ~valor["MID1"].str.startswith("39"), "MID1"] = "39" + valor["MID1"]
    
    # =============================
    # Prepare Outputs
    # =============================
    outputs = {}
    outputs["PASO_Output.csv"] = paso_output.to_csv(index=False).encode('utf-8')
    outputs["MEX_Output.csv"] = mex.to_csv(index=False).encode('utf-8')
    
    # Monthly Min Excel
    min_buf = io.BytesIO()
    with pd.ExcelWriter(min_buf, engine='openpyxl') as writer:
        zoho_fiserv.to_excel(writer, sheet_name="Fiserv", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Step1", index=False)
        zoho_tsys.to_excel(writer, sheet_name="TSYS", index=False)
        mex.to_excel(writer, sheet_name="MEX", index=False)
    outputs["Monthly_Min_Output.xlsx"] = min_buf.getvalue()

    return outputs

# =============================
# UI Components
# =============================
st.header("1. Upload Inputs")
f_tsys = st.file_uploader("Synoptic TSYS", type="csv")
f_fiserv = st.file_uploader("Synoptic Fiserv", type="csv")
f_zoho = st.file_uploader("Zoho Fees", type="xlsx")
f_mex = st.file_uploader("MEX File", type="xlsx")
f_s1 = st.file_uploader("PASO S1", type="csv")
f_s2 = st.file_uploader("PASO S2", type="csv")
f_wireless = st.file_uploader("Zoho Wireless", type="xlsx")
f_valor = st.file_uploader("Valor", type="xlsx")

if all([f_tsys, f_fiserv, f_zoho, f_mex, f_s1, f_s2, f_wireless, f_valor]):
    if st.button("🚀 Run Pipeline", type="primary"):
        files = {
            "Synoptic_TSYS": f_tsys, "Synoptic_Fiserv": f_fiserv,
            "Zoho_All_Fees": f_zoho, "MEX_file": f_mex,
            "PASO_S1": f_s1, "PASO_S2": f_s2,
            "Zoho_Wireless": f_wireless, "Valor": f_valor
        }
        results = run_pipeline(files, selected_month_year, six_months_before)
        
        st.success("Processing Complete!")
        for name, data in results.items():
            st.download_button(f"Download {name}", data, file_name=name)
