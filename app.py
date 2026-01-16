import io
import zipfile
import calendar
from datetime import datetime
import pandas as pd
import streamlit as st
from pandas.tseries.offsets import MonthEnd, DateOffset

# =============================
# Page Setup
# =============================
st.set_page_config(
    page_title="Residuals Data Cleaning Pipeline",
    page_icon="🔧",
    layout="wide"
)

st.title("🔧 Residuals Data Cleaning Pipeline")
st.markdown("---")

# =============================
# Helper Functions
# =============================

def clean_nbsp(df):
    """Remove non-breaking spaces and special characters"""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = (
                df[col]
                .fillna("")
                .str.replace("\xa0", "", regex=False)
                .str.replace("Â", "", regex=False)
                .str.strip()
            )
    return df


def clean_tsys_synoptic(df, selected_month_year, six_months_before):
    """Clean TSYS Synoptic data"""
    # Convert date columns
    cols = ["Date Opened", "Date Closed", "Last Deposit Date"]
    df[cols] = df[cols].apply(pd.to_datetime, errors="coerce")
    
    # 1) REMOVE: Date Opened > selected_month_year
    mask_remove = (df["Date Opened"] > selected_month_year)
    removed_tsys = df.loc[mask_remove].copy()
    kept_tsys = df.loc[~mask_remove].copy()
    
    # 2) Reopen if closed AND Date Closed > selected_month_year
    mask_reopen = (
        kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed") &
        (kept_tsys["Date Closed"] > selected_month_year)
    )
    kept_tsys.loc[mask_reopen, "Status"] = "Open"
    
    # 3) Remove closed with no/old deposit
    status_closed = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed")
    mask_no_deposit = kept_tsys["Last Deposit Date"].isna()
    mask_old_deposit = kept_tsys["Last Deposit Date"] <= six_months_before
    mask_remove_2 = status_closed & (mask_no_deposit | mask_old_deposit)
    
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_2]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_2].copy()
    
    # 4) Remove specific statuses
    statuses_to_remove = {"closed", "declined", "cancelled"}
    mask_remove_3 = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().isin(statuses_to_remove)
    
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_3]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_3].copy()
    
    # 5) Hard remove by Rep Name
    sa = kept_tsys["Rep Name"].fillna("").astype(str).str.strip().str.lower()
    Agent_hard_remove = {"hubwallet", "stephany perez", "nigel westbury", "brandon casillas"}
    mask_hard_remove = sa.isin(Agent_hard_remove)
    
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_hard_remove]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_hard_remove].copy()
    
    # Remove duplicates
    kept_tsys = kept_tsys.drop_duplicates(subset=["Merchant ID"], keep="first").copy()
    
    return kept_tsys, removed_tsys


def clean_fiserv_synoptic(df, paso_df, selected_month_year, six_months_before):
    """Clean Fiserv Synoptic data"""
    df = clean_nbsp(df)
    df = df.drop_duplicates(subset="Merchant #")
    
    # Convert dates
    cols = ["Open Date", "Close Date", "Last Batch Activity"]
    df[cols] = df[cols].apply(pd.to_datetime, errors="coerce")
    
    # 1) Remove: Open Date > selected_month_year
    mask_remove = (df["Open Date"] > selected_month_year)
    removed_fiserv = df.loc[mask_remove].copy()
    Kept_fiserv = df.loc[~mask_remove].copy()
    
    # 2) Reopen if close AND Close Date > selected_month_year
    mask_reopen = (
        Kept_fiserv["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close") &
        (Kept_fiserv["Close Date"] > selected_month_year)
    )
    Kept_fiserv.loc[mask_reopen, "Merchant Status"] = "Open"
    
    # 3) Sales Agent filtering
    Agent_to_keep = {"2030", "3030", "4030", "5030"}
    sa = Kept_fiserv["Sales Agent"].fillna("").astype(str).str.strip()
    
    mask_numeric = sa.str.fullmatch(r"\d+", na=False)
    mask_has_letter = sa.str.contains(r"[A-Za-z]", regex=True, na=False)
    mask_numeric_and_keep = mask_numeric & sa.isin(Agent_to_keep)
    
    mask_keep_agent = mask_has_letter | mask_numeric_and_keep
    mask_remove_agent = ~mask_keep_agent
    
    removed_fiserv = pd.concat([removed_fiserv, Kept_fiserv.loc[mask_remove_agent]], ignore_index=False)
    Kept_fiserv = Kept_fiserv.loc[~mask_remove_agent].copy()
    
    # Hard remove IS02
    sa = Kept_fiserv["Sales Agent"].fillna("").astype(str).str.strip()
    Agent_hard_remove = {"IS02"}
    mask_hard_remove = sa.isin(Agent_hard_remove)
    
    removed_fiserv = pd.concat([removed_fiserv, Kept_fiserv.loc[mask_hard_remove]], ignore_index=False)
    Kept_fiserv = Kept_fiserv.loc[~mask_hard_remove].copy()
    
    # Clean Merchant #
    Kept_fiserv["Merchant #"] = (
        Kept_fiserv["Merchant #"]
        .astype(str)
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.replace(r"\D+", "", regex=True)
    )
    
    # 4) CRITICAL: Keep closed merchants that exist in PASO
    status_close = (Kept_fiserv["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close"))
    closed_subset = Kept_fiserv.loc[status_close].copy()
    
    # Get PASO merchants
    paso_merchants = paso_df["MerchantNumber"].dropna().astype(str).str.strip().unique()
    
    # Check if closed merchants exist in PASO
    closed_in_paso = closed_subset["Merchant #"].isin(paso_merchants)
    
    # Remove closed NOT in PASO
    removed_fiserv = pd.concat([removed_fiserv, closed_subset.loc[~closed_in_paso]], ignore_index=False)
    
    # Keep: all OPEN + CLOSED in PASO
    Kept_fiserv = Kept_fiserv.loc[
        (~status_close) | (Kept_fiserv["Merchant #"].isin(paso_merchants))
    ].copy()
    
    return Kept_fiserv, removed_fiserv


def process_paso(paso_s1_df, paso_s2_df, kept_fiserv_df):
    """Process PASO files"""
    paso_s1 = clean_nbsp(paso_s1_df)
    paso_s2 = clean_nbsp(paso_s2_df)
    
    PASO = pd.concat([paso_s1, paso_s2], ignore_index=True)
    
    # Clean merchant numbers
    kept_fiserv_df['Merchant #'] = (
        kept_fiserv_df['Merchant #'].astype("string").str.replace("\xa0", "", regex=False).str.strip()
    )
    PASO["MerchantNumber"] = PASO["MerchantNumber"].astype("string").str.strip()
    
    # Keep only PASO merchants in Fiserv
    PASO_kept = PASO[PASO['MerchantNumber'].isin(kept_fiserv_df['Merchant #'])]
    
    return PASO_kept


def clean_zoho(df, selected_month_year, six_months_before):
    """Clean Zoho Reports"""
    # Internal agents list (empty per notebook)
    internal_agents = []
    internal_set = {str(x).strip() for x in internal_agents}
    
    # Normalize columns
    df["Sales Id"] = df["Sales Id"].fillna("").astype("string").str.strip()
    df["Merchant Number"] = df["Merchant Number"].fillna("").astype("string").str.strip()
    df["Account Status"] = df["Account Status"].fillna("").astype("string").str.strip()
    
    # Date columns
    cols = ["Date Approved", "Date Closed"]
    df[cols] = df[cols].apply(pd.to_datetime, errors="coerce")
    
    Zoho_kept = df.copy()
    Zoho_remove = df.iloc[0:0].copy()
    
    # 1) Sales Id filtering
    mask_numeric = Zoho_kept["Sales Id"].str.fullmatch(r"\d+")
    mask_has_letter = Zoho_kept["Sales Id"].str.contains(r"[A-Za-z]", regex=True)
    mask_numeric_and_internal = mask_numeric & Zoho_kept["Sales Id"].isin(internal_set)
    
    mask_keep_salesid = mask_has_letter | mask_numeric_and_internal
    mask_remove_salesid = ~mask_keep_salesid
    
    Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask_remove_salesid]], ignore_index=False)
    Zoho_kept = Zoho_kept[~mask_remove_salesid].copy()
    
    # 2) Date Approved > selected_month_year
    mask_remove_date = (Zoho_kept["Date Approved"] > selected_month_year)
    Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask_remove_date]], ignore_index=False)
    Zoho_kept = Zoho_kept[~mask_remove_date].copy()
    
    # 3) Reopen closed if Date Closed > selected_month_year
    mask_reopen = (
        (Zoho_kept["Account Status"].str.lower() == "closed") &
        (Zoho_kept["Date Closed"] > selected_month_year)
    )
    Zoho_kept.loc[mask_reopen, "Account Status"] = "Approved"
    
    # 4) Remove certain statuses
    statuses_to_remove = ["closed", "declined", "n/a", ""]
    mask_remove_status = Zoho_kept["Account Status"].str.lower().isin(statuses_to_remove)
    Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask_remove_status]], ignore_index=False)
    Zoho_kept = Zoho_kept[~mask_remove_status].copy()
    
    # 5) Remove numeric Sales Id except internal
    sid = Zoho_kept["Sales Id"].astype("string").str.strip()
    mask_numeric_sid = sid.str.fullmatch(r"\d+")
    mask_remove_numeric = mask_numeric_sid & (~sid.isin(internal_set))
    Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask_remove_numeric]], ignore_index=False)
    Zoho_kept = Zoho_kept[~mask_remove_numeric].copy()
    
    # 6) Hard remove is20
    sid2 = Zoho_kept["Sales Id"].astype("string").str.strip().str.lower()
    Zoho_hard_remove = {"is20"}
    mask_hard = sid2.isin(Zoho_hard_remove)
    Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask_hard]], ignore_index=False)
    Zoho_kept = Zoho_kept[~mask_hard].copy()
    
    return Zoho_kept, Zoho_remove


def filter_zoho_by_processors(zoho_df, tsys_df, fiserv_df):
    """Keep Zoho only if in TSYS or Fiserv"""
    def clean_id_numeric(s):
        s = s.astype("string").str.strip()
        s = s.str.replace(r"\.0+$", "", regex=True)
        s = s.str.replace(r"\D+", "", regex=True)
        s = s.replace("", pd.NA)
        return s
    
    zoho_ids = clean_id_numeric(zoho_df["Merchant Number"])
    tsys_ids = clean_id_numeric(tsys_df["Merchant ID"])
    fiserv_ids = clean_id_numeric(fiserv_df["Merchant #"])
    
    valid_merchants = pd.Index(tsys_ids.dropna().unique()).union(
        pd.Index(fiserv_ids.dropna().unique())
    )
    
    mask_keep = zoho_ids.isin(valid_merchants)
    
    # Also remove specific agents
    agents_to_remove = ['IS20', 'IS21', 'IS22', 'IS23', 'IS24']
    zoho_final_kept = zoho_df.loc[mask_keep].copy()
    zoho_final_kept = zoho_final_kept[~zoho_final_kept["Sales Id"].isin(agents_to_remove)].copy()
    
    zoho_final_remove = zoho_df.loc[~mask_keep].copy()
    
    return zoho_final_kept, zoho_final_remove


def create_monthly_min_zoho(zoho_kept_df, selected_month_year):
    """Format Zoho for Monthly Min output"""
    zoho_kept_df = zoho_kept_df.copy()
    
    # Add required columns
    zoho_kept_df["Recurring Fee Code"] = 2
    zoho_kept_df["Step 1"] = ""
    
    # Rename
    zoho_kept_df = zoho_kept_df.rename(columns={
        "Annual PCI Fee Month to Charge": "Recurring Fee Month",
        "Monthly Minimum MPA": "Monthly Minimum"
    })
    
    # PCI Count
    zoho_kept_df["Recurring Fee Month"] = (
        pd.to_numeric(zoho_kept_df["Recurring Fee Month"], errors="coerce").fillna(0).astype(int)
    )
    
    selected_month = selected_month_year.month
    zoho_kept_df["PCI Count"] = (zoho_kept_df["Recurring Fee Month"] == selected_month).astype(int)
    
    # Keep final columns
    final_cols = [
        "Processor", "Outside Agents", "Sales Id", "Merchant Number", "Account Name",
        "Account Status", "Date Approved", "Date Closed", "Recurring Fee Code",
        "Recurring Fee Month", "PCI Count", "PCI Amnt", "Monthly Minimum", "Step 1"
    ]
    
    zoho_kept_df = zoho_kept_df.loc[:, final_cols].copy()
    
    # Format dates
    zoho_kept_df["Date Approved"] = pd.to_datetime(zoho_kept_df["Date Approved"]).dt.strftime("%m/%d/%Y")
    zoho_kept_df["Date Closed"] = pd.to_datetime(zoho_kept_df["Date Closed"]).dt.strftime("%m/%d/%Y")
    
    # Split by processor
    proc = zoho_kept_df["Processor"].fillna("").astype(str).str.strip().str.lower()
    Zoho_keep_Fiserv = zoho_kept_df.loc[proc.eq("fiserv")].copy()
    Zoho_keep_TSYS = zoho_kept_df.loc[proc.eq("tsys")].copy()
    
    return Zoho_keep_Fiserv, Zoho_keep_TSYS


def clean_mex(df, six_months_before):
    """Clean MEX file"""
    df = df.copy()
    df["sales_rep_number"] = df["sales_rep_number"].astype(str).str.strip()
    
    # Remove closed
    mask_remove = df["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
    kept_mex = df.loc[~mask_remove].copy()
    
    # Remove specific sales reps
    sales_rep_numbers = ["HUBW-0000000006", "HUBW-0000000124", "HUBW-0000000024"]
    mask_remove_rep = kept_mex["sales_rep_number"].isin(sales_rep_numbers)
    kept_mex = kept_mex.loc[~mask_remove_rep].copy()
    
    return kept_mex


def add_step1_to_zoho_tsys(zoho_tsys_df, mex_df):
    """Add Step 1 values from MEX to Zoho TSYS"""
    mex_cols = [
        "visa_base_rate_discount_rev", "mc_base_rate_discount_rev",
        "disc_base_rate_discount_rev", "amex_base_rate_discount_rev"
    ]
    
    mex_df = mex_df.copy()
    mex_df[mex_cols] = mex_df[mex_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    
    mex_df["merchant_id_clean"] = mex_df["merchant_id"].astype(str).str.strip()
    mex_df["mex_step1"] = mex_df[mex_cols].sum(axis=1)
    
    mex_step1_lookup = mex_df.groupby("merchant_id_clean")["mex_step1"].sum()
    
    zoho_tsys_df = zoho_tsys_df.copy()
    zoho_tsys_df["Merchant_clean"] = zoho_tsys_df["Merchant Number"].astype(str).str.strip()
    zoho_tsys_df["Step 1"] = zoho_tsys_df["Merchant_clean"].map(mex_step1_lookup).fillna(0)
    zoho_tsys_df = zoho_tsys_df.drop(columns=["Merchant_clean"], errors="ignore")
    
    return zoho_tsys_df


def process_valor_step1(valor_df, zoho_wireless_df, tsys_df, fiserv_df):
    """Process Valor for Step 1"""
    # Clean MIDs
    for col in ["MID1", "MID2", "PROCESSOR"]:
        valor_df[col] = valor_df[col].fillna("").astype(str).str.strip()
    
    valor_df["MID1"] = valor_df["MID1"].str.replace(r"\.0+$", "", regex=True)
    valor_df["MID2"] = valor_df["MID2"].str.replace(r"\.0+$", "", regex=True)
    
    # Add 39 prefix for TSYS
    valor_df["Processor"] = valor_df["PROCESSOR"].str.lower()
    cond_tsys = valor_df["Processor"].str.startswith("tsys")
    
    mask_mid1 = cond_tsys & valor_df["MID1"].ne("")
    valor_df.loc[mask_mid1, "MID1"] = "39" + valor_df.loc[mask_mid1, "MID1"]
    
    mask_mid2 = cond_tsys & valor_df["MID1"].eq("") & valor_df["MID2"].ne("")
    valor_df.loc[mask_mid2, "MID2"] = "39" + valor_df.loc[mask_mid2, "MID2"]
    
    # Remove Webb/Mailbox Plus
    dba_norm = valor_df["DBA NAME"].fillna("").astype(str).str.strip().str.lower()
    mask_webb = (dba_norm.str.startswith("webb")) | (dba_norm == "mailbox plus")
    valor_df = valor_df.loc[~mask_webb].copy()
    
    # Keep only if in Fiserv or TSYS
    fiserv_df["Merchant #"] = fiserv_df["Merchant #"].fillna("").astype(str).str.strip()
    tsys_df["Merchant ID"] = tsys_df["Merchant ID"].fillna("").astype(str).str.strip()
    valor_df["MID1"] = valor_df["MID1"].fillna("").astype(str).str.strip()
    valor_df["MID2"] = valor_df["MID2"].fillna("").astype(str).str.strip()
    
    allowed_merchants = set(fiserv_df["Merchant #"]) | set(tsys_df["Merchant ID"])
    allowed_merchants.discard("")
    
    mask_keep = valor_df["MID1"].isin(allowed_merchants) | valor_df["MID2"].isin(allowed_merchants)
    valor_df = valor_df.loc[mask_keep].copy()
    
    # Add wireless count
    wireless_lookup = (
        zoho_wireless_df[["Merchant Number", "Wireless Count"]]
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .set_index("Merchant Number")["Wireless Count"]
    )
    
    L_col_name = valor_df.columns[11]
    valor_df["_L_clean"] = valor_df[L_col_name].astype("string").str.extract(r"(\d+)")[0]
    valor_df["Wireless count"] = valor_df["_L_clean"].map(wireless_lookup)
    
    valor_df = valor_df.drop(columns=["_L_clean", "Processor"], errors="ignore")
    
    return valor_df


def process_wireless_count(zoho_wireless_df):
    """Process wireless count from Zoho wireless report"""
    # Column A contains merchant number and count in format: 123456(X)
    colA = zoho_wireless_df.columns[0]
    
    A = zoho_wireless_df[colA].astype("string")
    mid_A = A.str.extract(r"(\d+)")[0]
    cnt_A = A.str.extract(r"\(\s*([^)]+)\s*\)")[0]
    
    lookup = pd.Series(cnt_A.values, index=mid_A).dropna()
    lookup = lookup[~lookup.index.duplicated(keep="first")]
    
    # Map to merchant numbers
    colF = "Merchant Number"
    mid_F = zoho_wireless_df[colF].astype("string").str.extract(r"(\d+)")[0]
    wireless_count = mid_F.map(lookup)
    
    result = pd.DataFrame({
        "Merchant Number": mid_F,
        "Account Name": zoho_wireless_df["Account Name"].astype("string"),
        "Wireless Count": wireless_count.astype("string")
    }).dropna(subset=["Merchant Number"]).drop_duplicates(subset=["Merchant Number"], keep="first").reset_index(drop=True)
    
    return result


# =============================
# Session State
# =============================
if 'step' not in st.session_state:
    st.session_state.step = 1

# =============================
# Sidebar - Date Selection
# =============================
with st.sidebar:
    st.header("📅 Date Selection")
    
    month_list = ["January", "February", "March", "April", "May", "June", 
                  "July", "August", "September", "October", "November", "December"]
    
    current_month_index = pd.Timestamp.today().month - 1
    current_year = pd.Timestamp.today().year
    
    selected_month = st.selectbox("Month", month_list, index=current_month_index)
    selected_year = st.number_input("Year", min_value=2020, max_value=2030, value=current_year)
    
    month_number = month_list.index(selected_month) + 1
    last_day = calendar.monthrange(selected_year, month_number)[1]
    selected_month_year = pd.Timestamp(f"{selected_year}-{month_number:02d}-{last_day}")
    six_months_before = selected_month_year - pd.DateOffset(months=6) + MonthEnd(0)
    
    st.info(f"**Selected:** {selected_month} {selected_year}\n\n**Date:** {selected_month_year.date()}")
    st.markdown("---")
    st.info(f"**Current Step:** {st.session_state.step}")

# =============================
# Step 1
# =============================
if st.session_state.step == 1:
    st.header("📁 Step 1: Upload Input Files")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fiserv_file = st.file_uploader("Synoptic — Fiserv", type=["csv", "xlsx"], key="fiserv")
        tsys_file = st.file_uploader("Synoptic — TSYS", type=["csv", "xlsx"], key="tsys")
        zoho_fees_file = st.file_uploader("Zoho — All Fees", type=["csv", "xlsx"], key="zoho_fees")
        zoho_wireless_file = st.file_uploader("Zoho — Wireless", type=["csv", "xlsx"], key="zoho_wireless")
    
    with col2:
        mex_file = st.file_uploader("MEX file", type=["csv", "xlsx"], key="mex")
        paso_s1_file = st.file_uploader("PASO S1", type=["csv", "xlsx"], key="paso_s1")
        paso_s2_file = st.file_uploader("PASO S2", type=["csv", "xlsx"], key="paso_s2")
        valor_file = st.file_uploader("Valor", type=["csv", "xlsx"], key="valor")
    
    all_files_uploaded = all([
        fiserv_file, tsys_file, zoho_fees_file, zoho_wireless_file,
        mex_file, paso_s1_file, paso_s2_file, valor_file
    ])
    
    if all_files_uploaded:
        st.success("✅ All 8 files uploaded successfully!")
        
        if st.button("🚀 Process Step 1", type="primary", use_container_width=True):
            with st.spinner("Processing files..."):
                try:
                    # Read files
                    fiserv_df = pd.read_csv(io.BytesIO(fiserv_file.getvalue()), skiprows=1, dtype={"Merchant #": "string"})
                    tsys_df = pd.read_csv(io.BytesIO(tsys_file.getvalue()))
                    zoho_fees_df = pd.read_excel(io.BytesIO(zoho_fees_file.getvalue()), skiprows=6, dtype={"Merchant Number": "string", "Sales Id": "string"})
                    zoho_wireless_df = pd.read_excel(io.BytesIO(zoho_wireless_file.getvalue()), skiprows=6)
                    mex_df = pd.read_excel(io.BytesIO(mex_file.getvalue()))
                    paso_s1_df = pd.read_csv(io.BytesIO(paso_s1_file.getvalue()), skiprows=1, dtype={"MerchantNumber": "string"})
                    paso_s2_df = pd.read_csv(io.BytesIO(paso_s2_file.getvalue()), skiprows=1, dtype={"MerchantNumber": "string"})
                    valor_df = pd.read_excel(io.BytesIO(valor_file.getvalue()), converters={"MID1": lambda x: "" if pd.isna(x) else str(x), "MID2": lambda x: "" if pd.isna(x) else str(x)})
                    
                    # Process PASO first (needed for Fiserv)
                    paso_s1_clean = clean_nbsp(paso_s1_df)
                    paso_s2_clean = clean_nbsp(paso_s2_df)
                    paso_combined = pd.concat([paso_s1_clean, paso_s2_clean], ignore_index=True)
                    
                    # Clean TSYS
                    kept_tsys, removed_tsys = clean_tsys_synoptic(tsys_df, selected_month_year, six_months_before)
                    
                    # Clean Fiserv (needs PASO)
                    kept_fiserv, removed_fiserv = clean_fiserv_synoptic(fiserv_df, paso_combined, selected_month_year, six_months_before)
                    
                    # PASO Output
                    paso_output = process_paso(paso_s1_df, paso_s2_df, kept_fiserv)
                    
                    # Clean Zoho
                    zoho_kept, zoho_removed = clean_zoho(zoho_fees_df, selected_month_year, six_months_before)
                    zoho_final_kept, zoho_final_removed = filter_zoho_by_processors(zoho_kept, kept_tsys, kept_fiserv)
                    
                    # Format Zoho for Monthly Min
                    zoho_fiserv, zoho_tsys = create_monthly_min_zoho
