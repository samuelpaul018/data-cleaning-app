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
    page_title="Residuals Data Cleaning Pipeline (Step 1)",
    page_icon="🧹",
    layout="wide",
)

st.title("🧹 Residuals Data Cleaning Pipeline — Step 1 (Final.ipynb EXACT)")
st.caption(
    "Generates: PASO_Output.csv, MEX_Output.csv, Valor_1ST_level_Output.xlsx, "
    "Monthly min and annual PCI without Step1 Output.xlsx"
)
st.markdown("---")


# =============================
# Session state (prevents reset on rerun/download)
# =============================
if "step1_outputs" not in st.session_state:
    st.session_state.step1_outputs = {}
if "step1_ran" not in st.session_state:
    st.session_state.step1_ran = False
if "last_run_meta" not in st.session_state:
    st.session_state.last_run_meta = {}


# =============================
# Sidebar: Month/Year
# =============================
with st.sidebar:
    st.header("⚙️ Settings")

    col1, col2 = st.columns(2)
    with col1:
        month = st.selectbox(
            "Month",
            list(range(1, 13)),
            index=datetime.now().month - 1,
            format_func=lambda m: datetime(2000, m, 1).strftime("%B"),
        )
    with col2:
        year = st.selectbox("Year", list(range(2020, 2031)), index=(datetime.now().year - 2020))

    last_day = calendar.monthrange(year, month)[1]
    selected_month_year = pd.Timestamp(year=year, month=month, day=last_day)
    six_months_before = selected_month_year - pd.DateOffset(months=6) + MonthEnd(0)

    st.info(f"Selected month-end: **{selected_month_year.date()}**")
    st.info(f"Six months before (month-end): **{six_months_before.date()}**")


# =============================
# Helpers
# =============================
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def read_csv_bytes(uploaded_file, **kwargs) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(uploaded_file.getvalue()), **kwargs)


def read_excel_bytes(uploaded_file, **kwargs) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(uploaded_file.getvalue()), **kwargs)


def clean_nbsp(df: pd.DataFrame) -> pd.DataFrame:
    # Matches the notebook behavior
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.replace("\xa0", "", regex=False)
                .str.replace("Â", "", regex=False)
                .str.strip()
            )
    return df


def clean_id_numeric(s: pd.Series) -> pd.Series:
    # Matches notebook in Zoho intersection step
    s = s.astype("string").str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)
    s = s.str.replace(r"\D+", "", regex=True)
    s = s.replace("", pd.NA)
    return s


# =============================
# TSYS synoptic cleaning (EXACT notebook cell 7)
# =============================
def clean_tsys_synoptic_exact(tsys_df: pd.DataFrame, selected_month_year: pd.Timestamp, six_months_before: pd.Timestamp) -> pd.DataFrame:
    df = tsys_df.copy()

    cols = ["Date Opened", "Date Closed", "Last Deposit Date"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # 1) remove Date Opened > cutoff
    mask_remove = df["Date Opened"] > selected_month_year
    removed_tsys = df.loc[mask_remove].copy()
    kept_tsys = df.loc[~mask_remove].copy()

    # 2) reopen if Status=closed AND Date Closed > cutoff
    mask_reopen = (
        kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed")
        & (kept_tsys["Date Closed"] > selected_month_year)
    )
    kept_tsys.loc[mask_reopen, "Status"] = "Open"

    # 3) remove closed AND (no deposit OR deposit <= six_months_before)
    status_closed = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed")
    mask_no_deposit = kept_tsys["Last Deposit Date"].isna()
    mask_old_deposit = kept_tsys["Last Deposit Date"] <= six_months_before
    mask_remove_2 = status_closed & (mask_no_deposit | mask_old_deposit)

    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_2]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_2].copy()

    # 4) remove statuses: closed, declined, cancelled
    statuses_to_remove = {"closed", "declined", "cancelled"}
    mask_remove_3 = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().isin(statuses_to_remove)

    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_3]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_3].copy()

    # 5) hard remove Rep Name
    sa = kept_tsys["Rep Name"].fillna("").astype(str).str.strip().str.lower()
    Agent_hard_remove = {"hubwallet", "stephany perez", "nigel westbury", "brandon casillas"}
    mask_hard_remove = sa.isin(Agent_hard_remove)

    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_hard_remove]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_hard_remove].copy()

    kept_tsys = kept_tsys.drop_duplicates(subset=["Merchant ID"], keep="first").copy()
    return kept_tsys


# =============================
# Fiserv synoptic cleaning (EXACT notebook cells 10 + 11)
# IMPORTANT: notebook DOES NOT remove close merchants by batch/close-date (those blocks are commented)
# =============================
def clean_fiserv_synoptic_exact(fiserv_df: pd.DataFrame, selected_month_year: pd.Timestamp) -> pd.DataFrame:
    df = fiserv_df.copy()
    df = clean_nbsp(df)
    df = df.drop_duplicates(subset="Merchant #")

    # date conversion
    cols = ["Open Date", "Close Date", "Last Batch Activity"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # 1) remove Open Date > cutoff
    mask_remove = df["Open Date"] > selected_month_year
    removed_fiserv = df.loc[mask_remove].copy()  # kept for parity, not used later
    kept = df.loc[~mask_remove].copy()

    # 2) reopen: close + Close Date > cutoff => Open
    mask_reopen = (
        kept["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close")
        & (kept["Close Date"] > selected_month_year)
    )
    kept.loc[mask_reopen, "Merchant Status"] = "Open"

    # 6) agent filtering
    Agent_to_keep = {"2030", "3030", "4030", "5030"}
    sa = kept["Sales Agent"].fillna("").astype(str).str.strip()

    mask_numeric = sa.str.fullmatch(r"\d+", na=False)
    mask_has_letter = sa.str.contains(r"[A-Za-z]", regex=True, na=False)
    mask_numeric_and_keep = mask_numeric & sa.isin(Agent_to_keep)

    mask_keep_agent = mask_has_letter | mask_numeric_and_keep
    mask_remove_agent = ~mask_keep_agent

    removed_fiserv = pd.concat([removed_fiserv, kept.loc[mask_remove_agent]], ignore_index=False)
    kept = kept.loc[~mask_remove_agent].copy()

    # hard remove IS02
    sa2 = kept["Sales Agent"].fillna("").astype(str).str.strip()
    mask_hard_remove = sa2.isin({"IS02"})
    removed_fiserv = pd.concat([removed_fiserv, kept.loc[mask_hard_remove]], ignore_index=False)
    kept = kept.loc[~mask_hard_remove].copy()

    # digits-only Merchant #
    kept["Merchant #"] = (
        kept["Merchant #"]
        .astype(str)
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.replace(r"\D+", "", regex=True)
    )

    return kept


# =============================
# Zoho processing (EXACT notebook cells 24, 26, 28)
# =============================
def process_zoho_exact(
    zoho_raw: pd.DataFrame,
    kept_tsys: pd.DataFrame,
    kept_fiserv: pd.DataFrame,
    selected_month_year: pd.Timestamp,
):
    Zoho = zoho_raw.copy()

    # dates
    cols = ["Date Approved", "Date Closed"]
    for c in cols:
        if c in Zoho.columns:
            Zoho[c] = pd.to_datetime(Zoho[c], errors="coerce")

    # internal agents empty in notebook
    internal_set = set()

    Zoho["Sales Id"] = Zoho["Sales Id"].fillna("").astype("string").str.strip()
    Zoho["Merchant Number"] = Zoho["Merchant Number"].fillna("").astype("string").str.strip()
    Zoho["Account Status"] = Zoho["Account Status"].fillna("").astype("string").str.strip()

    Zoho_kept = Zoho.copy()
    Zoho_remove = Zoho.iloc[0:0].copy()

    def move_rows(mask, _label):
        nonlocal Zoho_kept, Zoho_remove
        if int(mask.sum()) > 0:
            Zoho_remove = pd.concat([Zoho_remove, Zoho_kept[mask]], ignore_index=False)
            Zoho_kept = Zoho_kept[~mask].copy()

    # FILTER 1: Sales Id keep letters OR numeric+internal
    mask_numeric = Zoho_kept["Sales Id"].str.fullmatch(r"\d+")
    mask_has_letter = Zoho_kept["Sales Id"].str.contains(r"[A-Za-z]", regex=True)
    mask_numeric_and_internal = mask_numeric & Zoho_kept["Sales Id"].isin(internal_set)
    mask_keep_salesid = mask_has_letter | mask_numeric_and_internal
    move_rows(~mask_keep_salesid, "Removed by Sales Id rule")

    # FILTER 2: Date Approved > cutoff remove
    move_rows(Zoho_kept["Date Approved"] > selected_month_year, "Removed by Date Approved rule")

    # FILTER 3: Closed + Date Closed > cutoff => Approved
    mask_change_to_open = (
        (Zoho_kept["Account Status"].str.lower() == "closed")
        & (Zoho_kept["Date Closed"] > selected_month_year)
    )
    Zoho_kept.loc[mask_change_to_open, "Account Status"] = "Approved"

    # FILTER 4: remove statuses
    statuses_to_remove = ["closed", "declined", "n/a", ""]
    move_rows(Zoho_kept["Account Status"].str.lower().isin(statuses_to_remove), "Removed by Account Status rule")

    # FILTER 5: remove numeric Sales Id except internal (internal empty)
    sid = Zoho_kept["Sales Id"].astype("string").str.strip()
    mask_numeric_sid = sid.str.fullmatch(r"\d+")
    move_rows(mask_numeric_sid & (~sid.isin(internal_set)), "Removed numeric Sales Id (except internal)")

    # FILTER 6: hard remove is20
    sid2 = Zoho_kept["Sales Id"].astype("string").str.strip().str.lower()
    move_rows(sid2.isin({"is20"}), "Hard removed Sales Id (is20)")

    # Intersection against TSYS/Fiserv
    ZOHO_COL, TSYS_COL, FISERV_COL = "Merchant Number", "Merchant ID", "Merchant #"

    zoho_ids_clean = clean_id_numeric(Zoho_kept[ZOHO_COL])
    tsys_ids_clean = clean_id_numeric(kept_tsys[TSYS_COL])
    fiserv_ids_clean = clean_id_numeric(kept_fiserv[FISERV_COL])

    valid_merchants = pd.Index(tsys_ids_clean.dropna().unique()).union(pd.Index(fiserv_ids_clean.dropna().unique()))
    mask_keep = zoho_ids_clean.isin(valid_merchants)

    agents_to_remove = ["IS20", "IS21", "IS22", "IS23", "IS24"]

    zoho_final_kept = Zoho_kept.loc[mask_keep].copy()
    zoho_final_remove = Zoho_kept.loc[~mask_keep].copy()

    zoho_final_kept = zoho_final_kept[~zoho_final_kept["Sales Id"].isin(agents_to_remove)].copy()

    # Format + split (cell 28)
    zoho_final_kept = zoho_final_kept.copy()
    zoho_final_kept["Recurring Fee Code"] = 2
    zoho_final_kept["Step 1"] = ""

    zoho_final_kept = zoho_final_kept.rename(
        columns={
            "Annual PCI Fee Month to Charge": "Recurring Fee Month",
            "Monthly Minimum MPA": "Monthly Minimum",
        }
    )

    zoho_final_kept["Recurring Fee Month"] = (
        pd.to_numeric(zoho_final_kept["Recurring Fee Month"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    selected_month = selected_month_year.month
    zoho_final_kept["PCI Count"] = (
        zoho_final_kept["Recurring Fee Month"].astype(str).str.strip()
        == str(selected_month).strip()
    ).astype(int)

    final_cols = [
        "Processor",
        "Outside Agents",
        "Sales Id",
        "Merchant Number",
        "Account Name",
        "Account Status",
        "Date Approved",
        "Date Closed",
        "Recurring Fee Code",
        "Recurring Fee Month",
        "PCI Count",
        "PCI Amnt",
        "Monthly Minimum",
        "Step 1",
    ]
    zoho_final_kept = zoho_final_kept.loc[:, final_cols].copy()

    zoho_final_kept["Date Approved"] = pd.to_datetime(zoho_final_kept["Date Approved"], errors="coerce").dt.strftime("%m/%d/%Y")
    zoho_final_kept["Date Closed"] = pd.to_datetime(zoho_final_kept["Date Closed"], errors="coerce").dt.strftime("%m/%d/%Y")

    proc = zoho_final_kept["Processor"].fillna("").astype(str).str.strip().str.lower()
    Zoho_keep_Fiserv = zoho_final_kept.loc[proc.eq("fiserv")].copy()
    Zoho_keep_TSYS = zoho_final_kept.loc[proc.eq("tsys")].copy()

    return Zoho_keep_Fiserv, Zoho_keep_TSYS, final_cols


# =============================
# MEX for monthly workbook (EXACT notebook cell 30)
# =============================
def mex_for_monthly_exact(mex_raw: pd.DataFrame) -> pd.DataFrame:
    MEX = mex_raw.copy()
    MEX["sales_rep_number"] = MEX["sales_rep_number"].astype(str).str.strip()

    mask_remove_status = MEX["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
    removed_mex = MEX.loc[mask_remove_status].copy()
    kept_mex = MEX.loc[~mask_remove_status].copy()

    sales_rep_numbers = ["HUBW-0000000006", "HUBW-0000000124", "HUBW-0000000024", "HUBW-0000000024"]
    mask_remove_rep = kept_mex["sales_rep_number"].isin(sales_rep_numbers)

    removed_mex = pd.concat([removed_mex, kept_mex.loc[mask_remove_rep]], ignore_index=True)
    kept_mex = kept_mex.loc[~mask_remove_rep].copy()

    return kept_mex


# Step1 lookup from MEX into Zoho_keep_TSYS (EXACT notebook inside cell 30)
def apply_mex_step1_lookup_exact(Zoho_keep_TSYS: pd.DataFrame, kept_mex: pd.DataFrame, final_cols: list[str]) -> pd.DataFrame:
    mex_cols = [
        "visa_base_rate_discount_rev",
        "mc_base_rate_discount_rev",
        "disc_base_rate_discount_rev",
        "amex_base_rate_discount_rev",
    ]

    kept_mex = kept_mex.copy()
    kept_mex[mex_cols] = kept_mex[mex_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    kept_mex["merchant_id_clean"] = kept_mex["merchant_id"].astype(str).str.strip()
    kept_mex["mex_step1"] = kept_mex[mex_cols].sum(axis=1)
    mex_step1_lookup = kept_mex.groupby("merchant_id_clean")["mex_step1"].sum()

    Zoho_keep_TSYS = Zoho_keep_TSYS.copy()
    Zoho_keep_TSYS["Merchant_clean"] = Zoho_keep_TSYS["Merchant Number"].astype(str).str.strip()
    Zoho_keep_TSYS["Step 1"] = Zoho_keep_TSYS["Merchant_clean"].map(mex_step1_lookup).fillna(0)

    Zoho_keep_TSYS = Zoho_keep_TSYS.drop(columns=["Merchant_clean"], errors="ignore")
    Zoho_keep_TSYS = Zoho_keep_TSYS[final_cols].copy()
    return Zoho_keep_TSYS


# =============================
# MEX output CSV (EXACT notebook cell 32)
# =============================
def mex_output_csv_exact(mex_raw: pd.DataFrame, six_months_before: pd.Timestamp) -> pd.DataFrame:
    MEX = mex_raw.copy()
    MEX["sales_rep_number"] = MEX["sales_rep_number"].astype(str).str.strip()

    mask_remove_status = MEX["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
    removed_mex = MEX.loc[mask_remove_status].copy()
    kept_mex_1 = MEX.loc[~mask_remove_status].copy()

    cols_to_check = ["total_settle_tickets", "net_settle_volume", "merchant_total_revenue", "STW_total_residual"]
    removed_mex[cols_to_check] = removed_mex[cols_to_check].apply(pd.to_numeric, errors="coerce")

    mask_back_to_kept = (
        removed_mex["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
        & (pd.to_datetime(removed_mex["last_deposit_date"], errors="coerce") < six_months_before)
        & (removed_mex[cols_to_check].fillna(0).ne(0).any(axis=1))
    )

    to_keep = removed_mex.loc[mask_back_to_kept].copy()
    to_remove = removed_mex.loc[~mask_back_to_kept].copy()

    kept_mex_1 = pd.concat([kept_mex_1, to_keep], ignore_index=True)
    removed_mex = to_remove

    sales_rep_numbers = ["HUBW-0000000006", "HUBW-0000000124"]
    mask_remove_rep = kept_mex_1["sales_rep_number"].isin(sales_rep_numbers)
    removed_mex = pd.concat([removed_mex, kept_mex_1.loc[mask_remove_rep]], ignore_index=True)
    kept_mex_1 = kept_mex_1.loc[~mask_remove_rep].copy()

    return kept_mex_1


# =============================
# Wireless Count sheet (EXACT notebook cell 39)
# =============================
def build_wireless_count_sheet_exact(wcv_raw: pd.DataFrame) -> pd.DataFrame:
    WCV = wcv_raw.copy()

    WCV.rename(columns={WCV.columns[0]: "Mer + wir"}, inplace=True)
    WCV.rename(columns={WCV.columns[5]: "Merchant Number"}, inplace=True)

    colA = "Mer + wir"
    col_map = {str(c).strip(): c for c in WCV.columns}
    colF = col_map["Merchant Number"]

    A = WCV[colA].astype("string")
    mid_A = A.str.extract(r"(\d+)")[0]
    cnt_A = A.str.extract(r"\(\s*([^)]+)\s*\)")[0]

    lookup = pd.Series(cnt_A.values, index=mid_A).dropna()
    lookup = lookup[~lookup.index.duplicated(keep="first")]

    mid_F = WCV[colF].astype("string").str.extract(r"(\d+)")[0]
    wireless_count = mid_F.map(lookup)

    result = (
        pd.DataFrame(
            {
                "Merchant Number": mid_F,
                "Account Name": WCV[col_map.get("Account Name", "Account Name")].astype("string"),
                "Wireless Count": wireless_count.astype("string"),
            }
        )
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .reset_index(drop=True)
    )
    return result


# =============================
# Valor processing (EXACT notebook cells 34, 35, 40)
# =============================
def process_valor_exact(valor_raw: pd.DataFrame, wireless_result: pd.DataFrame, kept_fiserv: pd.DataFrame, kept_tsys: pd.DataFrame) -> pd.DataFrame:
    Valor = valor_raw.copy()

    for col in ["MID1", "MID2", "PROCESSOR"]:
        if col in Valor.columns:
            Valor[col] = Valor[col].fillna("").astype(str).str.strip()

    # remove trailing .0 only (NOT digits-only)
    Valor["MID1"] = Valor["MID1"].astype(str).str.replace(r"\.0+$", "", regex=True)
    Valor["MID2"] = Valor["MID2"].astype(str).str.replace(r"\.0+$", "", regex=True)

    # Processor lowercase
    Valor["Processor"] = Valor["PROCESSOR"].fillna("").astype(str).str.lower()

    # TSYS: ALWAYS prefix 39 (exact notebook)
    cond_tsys = Valor["Processor"].str.startswith("tsys")
    mask_mid1 = cond_tsys & Valor["MID1"].ne("")
    Valor.loc[mask_mid1, "MID1"] = "39" + Valor.loc[mask_mid1, "MID1"]

    mask_mid2 = cond_tsys & Valor["MID1"].eq("") & Valor["MID2"].ne("")
    Valor.loc[mask_mid2, "MID2"] = "39" + Valor.loc[mask_mid2, "MID2"]

    # DBA filter
    dba_norm = Valor["DBA NAME"].fillna("").astype(str).str.strip().str.lower()
    mask_webb = (dba_norm.str.startswith("webb")) | (dba_norm == "mailbox plus")
    Valor = Valor.loc[~mask_webb].copy()

    # Allowed set (exact notebook)
    Kept_fiserv = kept_fiserv.copy()
    kept_tsys = kept_tsys.copy()

    Kept_fiserv["Merchant #"] = Kept_fiserv["Merchant #"].fillna("").astype(str).str.strip()
    kept_tsys["Merchant ID"] = kept_tsys["Merchant ID"].fillna("").astype(str).str.strip()

    Valor["MID1"] = Valor["MID1"].fillna("").astype(str).str.strip()
    Valor["MID2"] = Valor["MID2"].fillna("").astype(str).str.strip()

    allowed_merchants = set(Kept_fiserv["Merchant #"]) | set(kept_tsys["Merchant ID"])
    allowed_merchants.discard("")

    mask_keep = Valor["MID1"].isin(allowed_merchants) | Valor["MID2"].isin(allowed_merchants)
    Valor = Valor.loc[mask_keep].copy()

    # Wireless XLOOKUP (exact notebook)
    wireless_lookup = (
        wireless_result[["Merchant Number", "Wireless Count"]]
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .set_index("Merchant Number")["Wireless Count"]
    )

    L_col_name = Valor.columns[11]  # column L (0-based 11)
    Valor["_L_clean"] = Valor[L_col_name].astype("string").str.extract(r"(\d+)")[0]

    # notebook adds a blank column named " "
    Valor[" "] = ""

    Valor["Wireless count"] = Valor["_L_clean"].map(wireless_lookup)

    aj_pos = 35
    col = Valor.pop("Wireless count")
    Valor.insert(min(aj_pos, len(Valor.columns)), "Wireless count", col)

    Valor.drop(columns=["_L_clean", "Processor"], inplace=True, errors="ignore")
    return Valor


# =============================
# Step-1 pipeline runner
# =============================
def run_step1_pipeline(files: dict) -> dict[str, bytes]:
    outputs: dict[str, bytes] = {}

    # TSYS
    tsys_raw = read_csv_bytes(files["Synoptic_TSYS"])
    kept_tsys = clean_tsys_synoptic_exact(tsys_raw, selected_month_year, six_months_before)

    # Fiserv (dtype EXACT)
    fiserv_raw = read_csv_bytes(files["Synoptic_Fiserv"], skiprows=1, dtype={"Merchant #": "string"})
    kept_fiserv = clean_fiserv_synoptic_exact(fiserv_raw, selected_month_year)

    # PASO (order EXACT: 3900 first, then 1800)
    paso_1800 = read_csv_bytes(files["PASO_1800"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_3900 = read_csv_bytes(files["PASO_3900"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_1800 = clean_nbsp(paso_1800)
    paso_3900 = clean_nbsp(paso_3900)
    PASO = pd.concat([paso_3900, paso_1800], ignore_index=True)

    kept_fiserv["Merchant #"] = kept_fiserv["Merchant #"].astype("string").str.replace("\xa0", "", regex=False).str.strip()
    PASO["MerchantNumber"] = PASO["MerchantNumber"].astype("string").str.strip()

    PASO_kept = PASO[PASO["MerchantNumber"].isin(kept_fiserv["Merchant #"])].copy()
    outputs["PASO_Output.csv"] = to_csv_bytes(PASO_kept)

    # Zoho
    zoho_raw = read_excel_bytes(files["Zoho_All_Fees"], skiprows=6, dtype={"Merchant Number": "string", "Sales Id": "string"})
    Zoho_keep_Fiserv, Zoho_keep_TSYS, final_cols = process_zoho_exact(zoho_raw, kept_tsys, kept_fiserv, selected_month_year)

    # MEX monthly workbook dataset + Step1 lookup
    mex_raw = read_excel_bytes(files["MEX_file"])
    kept_mex_monthly = mex_for_monthly_exact(mex_raw)
    Zoho_keep_TSYS = apply_mex_step1_lookup_exact(Zoho_keep_TSYS, kept_mex_monthly, final_cols)

    # Monthly min workbook (exact sheet names/order)
    monthly_buf = io.BytesIO()
    with pd.ExcelWriter(monthly_buf, engine="openpyxl") as writer:
        Zoho_keep_Fiserv.to_excel(writer, sheet_name="Fiserv", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Step1", index=False)
        Zoho_keep_TSYS.to_excel(writer, sheet_name="TSYS", index=False)

        kept_mex_sheet = kept_mex_monthly.drop(columns=["merchant_id_clean", "mex_step1"], errors="ignore")
        kept_mex_sheet.to_excel(writer, sheet_name="MEX", index=False)

    monthly_buf.seek(0)
    outputs["Monthly min and annual PCI without Step1 Output.xlsx"] = monthly_buf.getvalue()

    # MEX output CSV (exact)
    mex_out_df = mex_output_csv_exact(mex_raw, six_months_before)
    outputs["MEX_Output.csv"] = to_csv_bytes(mex_out_df)

    # Wireless
    wireless_raw = read_excel_bytes(files["Zoho_Wireless"], skiprows=6)
    wireless_result = build_wireless_count_sheet_exact(wireless_raw)

    # Valor
    valor_raw = read_excel_bytes(
        files["Valor"],
        converters={
            "MID1": lambda x: "" if pd.isna(x) else str(x),
            "MID2": lambda x: "" if pd.isna(x) else str(x),
            "PROCESSOR": lambda x: "" if pd.isna(x) else str(x),
            "DBA NAME": lambda x: "" if pd.isna(x) else str(x),
        },
    )
    valor_iso = process_valor_exact(valor_raw, wireless_result, kept_fiserv, kept_tsys)

    valor_buf = io.BytesIO()
    with pd.ExcelWriter(valor_buf, engine="openpyxl") as writer:
        valor_iso.drop(columns=["MID1_clean"], errors="ignore").to_excel(writer, sheet_name="ISO Report", index=False)
        wireless_result.to_excel(writer, sheet_name="Wireless Count", index=False)
    valor_buf.seek(0)
    outputs["Valor_1ST_level_Output.xlsx"] = valor_buf.getvalue()

    return outputs


def make_zip_bytes(outputs: dict[str, bytes]) -> bytes:
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname, data in outputs.items():
            zf.writestr(fname, data)
    zbuf.seek(0)
    return zbuf.getvalue()


# =============================
# UI Uploaders (8 inputs)
# =============================
st.header("📁 Step 1: Upload 8 Raw Input Files (Notebook naming parity)")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Synoptic")
    f_tsys = st.file_uploader("Synoptic — TSYS (CSV)  [Synoptic_Tsys.csv]", type=["csv"])
    f_fiserv = st.file_uploader("Synoptic — Fiserv (CSV)  [Synoptic_Fiserv.csv]", type=["csv"])

    st.subheader("Zoho")
    f_zoho = st.file_uploader("Zoho — Reports (XLSX)  [Zoho_Reports.xlsx]", type=["xlsx"])
    f_wireless = st.file_uploader("Wireless Report (XLSX)  [Wireless Report - New (IRIS).xlsx]", type=["xlsx"])

with col2:
    st.subheader("Other")
    f_mex = st.file_uploader("MEX file (XLSX)  [MEX_XX.xlsx]", type=["xlsx"])
    # IMPORTANT: notebook concat order is 3900 first, then 1800
    f_1800 = st.file_uploader("PASO 1800 (CSV)  [S1_.._1800.csv]", type=["csv"])
    f_3900 = st.file_uploader("PASO 3900 (CSV)  [S2_.._3900.csv]", type=["csv"])
    f_valor = st.file_uploader("Valor Step1 (XLSX)  [Valor_Step1.xlsx]", type=["xlsx"])

files = {
    "Synoptic_TSYS": f_tsys,
    "Synoptic_Fiserv": f_fiserv,
    "Zoho_All_Fees": f_zoho,
    "Zoho_Wireless": f_wireless,
    "MEX_file": f_mex,
    "PASO_1800": f_1800,
    "PASO_3900": f_3900,
    "Valor": f_valor,
}

missing = [k for k, v in files.items() if v is None]
st.markdown("---")

c1, c2 = st.columns([1, 1])
with c1:
    if st.button("🧹 Clear outputs / Start over", use_container_width=True):
        st.session_state.step1_outputs = {}
        st.session_state.step1_ran = False
        st.session_state.last_run_meta = {}
        st.rerun()

with c2:
    if st.session_state.step1_ran and st.session_state.last_run_meta:
        st.info(f"Last run: {st.session_state.last_run_meta.get('label', 'Unknown')}")

if missing:
    st.warning("⚠️ Missing: " + ", ".join(missing))
else:
    st.success("✅ All 8 files uploaded.")

    if st.button("🚀 Generate Step-1 Outputs", type="primary", use_container_width=True):
        with st.spinner("Running Step-1 pipeline (Final.ipynb exact)..."):
            try:
                outputs = run_step1_pipeline(files)
                st.session_state.step1_outputs = outputs
                st.session_state.step1_ran = True
                st.session_state.last_run_meta = {
                    "label": f"{selected_month_year.strftime('%B %Y')} ({datetime.now().strftime('%H:%M:%S')})"
                }
                st.success("✅ Outputs generated and saved. You can download without losing state.")
            except Exception as e:
                st.error(f"❌ Step-1 pipeline failed: {e}")

# =============================
# Download section (persists across reruns)
# =============================
if st.session_state.step1_ran and st.session_state.step1_outputs:
    st.markdown("---")
    st.subheader("📥 Download Outputs")

    outputs = st.session_state.step1_outputs

    zip_bytes = make_zip_bytes(outputs)
    st.download_button(
        label="⬇️ Download ALL outputs (ZIP)",
        data=zip_bytes,
        file_name="Step1_Outputs.zip",
        mime="application/zip",
        use_container_width=True,
    )

    st.markdown("### Or download individually")

    order = [
        "PASO_Output.csv",
        "MEX_Output.csv",
        "Valor_1ST_level_Output.xlsx",
        "Monthly min and annual PCI without Step1 Output.xlsx",
    ]
    for name in order:
        if name not in outputs:
            continue
        mime = "text/csv" if name.endswith(".csv") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        st.download_button(
            label=f"⬇️ {name}",
            data=outputs[name],
            file_name=name,
            mime=mime,
            use_container_width=True,
        )

st.markdown("---")
st.caption("Residuals Data Cleaning Pipeline — Step 1 (Final.ipynb EXACT) • Persistent downloads + ZIP")

