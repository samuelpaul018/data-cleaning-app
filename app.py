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

st.title("🧹 Residuals Data Cleaning Pipeline — Step 1 (Output-Matching)")
st.caption(
    "Generates: PASO_Output.csv, MEX_Output.csv, Valor_1ST_level_Output.xlsx, "
    "Monthly min and annual PCI without Step1 Output.xlsx"
)
st.markdown("---")


# =============================
# Session state
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


def digits_only_series(s: pd.Series) -> pd.Series:
    s = s.astype("string").fillna("").str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)
    s = s.str.replace(r"\D+", "", regex=True)
    return s


def excel_mid_to_string(x) -> str:
    """
    Robust MID conversion for Valor:
    handles floats, ints, scientific notation, and '...0' endings.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    # If looks like scientific notation or float-like, try numeric conversion
    try:
        val = float(s)
        if pd.isna(val):
            return ""
        # Convert to int safely (MIDs are whole numbers)
        sval = str(int(round(val)))
        return sval
    except Exception:
        pass
    # fallback: strip .0 and non-digits
    s = s.replace("\xa0", "").strip()
    s = pd.Series([s]).astype("string").str.replace(r"\.0+$", "", regex=True).iloc[0]
    s = pd.Series([s]).astype("string").str.replace(r"\D+", "", regex=True).iloc[0]
    return str(s)


# =============================
# TSYS synoptic cleaning (keep close logic like your original pipeline)
# =============================
def clean_tsys_synoptic(tsys_df: pd.DataFrame, selected_month_year: pd.Timestamp, six_months_before: pd.Timestamp) -> pd.DataFrame:
    df = tsys_df.copy()

    for c in ["Date Opened", "Date Closed", "Last Deposit Date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # remove merchants opened after cutoff
    if "Date Opened" in df.columns:
        df = df.loc[~(df["Date Opened"] > selected_month_year)].copy()

    # reopen future-closed
    if "Status" in df.columns and "Date Closed" in df.columns:
        mask = (df["Status"].astype(str).str.strip().str.lower() == "closed") & (df["Date Closed"] > selected_month_year)
        df.loc[mask, "Status"] = "Open"

    # remove closed with old/no deposits (<= six_months_before)
    if "Status" in df.columns and "Last Deposit Date" in df.columns:
        status_closed = df["Status"].astype(str).str.strip().str.lower().eq("closed")
        mask_remove = status_closed & (df["Last Deposit Date"].isna() | (df["Last Deposit Date"] <= six_months_before))
        df = df.loc[~mask_remove].copy()

    # remove certain statuses
    if "Status" in df.columns:
        rm = {"closed", "declined", "cancelled"}
        df = df.loc[~df["Status"].astype(str).str.strip().str.lower().isin(rm)].copy()

    # hard rep removals
    if "Rep Name" in df.columns:
        hard = {"hubwallet", "stephany perez", "nigel westbury", "brandon casillas"}
        df = df.loc[~df["Rep Name"].astype(str).str.strip().str.lower().isin(hard)].copy()

    if "Merchant ID" in df.columns:
        df = df.drop_duplicates(subset=["Merchant ID"], keep="first").copy()

    return df


# =============================
# Fiserv synoptic cleaning (THIS is the key for PASO + Monthly workbook)
# Make Merchant # digits-only BEFORE PASO filtering.
# DO NOT mass-drop close merchants (this was breaking your match).
# =============================
def clean_fiserv_synoptic(fiserv_df: pd.DataFrame, selected_month_year: pd.Timestamp) -> pd.DataFrame:
    df = fiserv_df.copy()
    df = clean_nbsp(df)

    if "Merchant #" in df.columns:
        df = df.drop_duplicates(subset=["Merchant #"])

    for c in ["Open Date", "Close Date", "Last Batch Activity"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # remove Open Date > cutoff
    if "Open Date" in df.columns:
        df = df.loc[~(df["Open Date"] > selected_month_year)].copy()

    # reopen close if close date is after cutoff
    if "Merchant Status" in df.columns and "Close Date" in df.columns:
        status_close = df["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close")
        mask = status_close & (df["Close Date"] > selected_month_year)
        df.loc[mask, "Merchant Status"] = "Open"

    # sales agent filtering (letters OR numeric in keep set)
    if "Sales Agent" in df.columns:
        agent_keep = {"2030", "3030", "4030", "5030"}
        sa = df["Sales Agent"].fillna("").astype(str).str.strip()

        mask_numeric = sa.str.fullmatch(r"\d+", na=False)
        mask_has_letter = sa.str.contains(r"[A-Za-z]", regex=True, na=False)
        mask_numeric_keep = mask_numeric & sa.isin(agent_keep)

        df = df.loc[mask_has_letter | mask_numeric_keep].copy()

        # hard remove IS02
        df = df.loc[~df["Sales Agent"].fillna("").astype(str).str.strip().isin({"IS02"})].copy()

    # Merchant # digits only (critical for PASO match)
    if "Merchant #" in df.columns:
        df["Merchant #"] = digits_only_series(df["Merchant #"])

    return df


# =============================
# PASO kept (match your PASO_Output Original behavior)
# =============================
def paso_output(paso_s1: pd.DataFrame, paso_s2: pd.DataFrame, kept_fiserv: pd.DataFrame) -> pd.DataFrame:
    paso = pd.concat([clean_nbsp(paso_s1), clean_nbsp(paso_s2)], ignore_index=True)

    paso["MerchantNumber"] = digits_only_series(paso["MerchantNumber"])
    kept_mid = digits_only_series(kept_fiserv["Merchant #"])

    out = paso.loc[paso["MerchantNumber"].isin(set(kept_mid))].copy()
    return out


# =============================
# Zoho processing tuned for your Monthly workbook output
# Key: Step1 lookup MUST use cleaned numeric Merchant IDs (else Step1 becomes all 0).
# =============================
def process_zoho_for_monthly(
    zoho_raw: pd.DataFrame,
    kept_tsys: pd.DataFrame,
    kept_fiserv: pd.DataFrame,
    selected_month_year: pd.Timestamp,
):
    z = zoho_raw.copy()

    for c in ["Date Approved", "Date Closed"]:
        if c in z.columns:
            z[c] = pd.to_datetime(z[c], errors="coerce")

    # enforce strings
    z["Sales Id"] = z.get("Sales Id", "").fillna("").astype("string").str.strip()
    z["Merchant Number"] = z.get("Merchant Number", "").fillna("").astype("string").str.strip()
    z["Account Status"] = z.get("Account Status", "").fillna("").astype("string").str.strip()

    # keep Sales Id with letters
    z = z.loc[z["Sales Id"].str.contains(r"[A-Za-z]", regex=True, na=False)].copy()

    # remove approved after cutoff
    if "Date Approved" in z.columns:
        z = z.loc[~(z["Date Approved"] > selected_month_year)].copy()

    # reopen closed if Date Closed after cutoff
    if "Date Closed" in z.columns:
        mask = (z["Account Status"].str.lower() == "closed") & (z["Date Closed"] > selected_month_year)
        z.loc[mask, "Account Status"] = "Approved"

    # remove unwanted statuses
    statuses_to_remove = {"closed", "declined", "n/a", ""}
    z = z.loc[~z["Account Status"].str.lower().isin(statuses_to_remove)].copy()

    # remove is20
    z = z.loc[~z["Sales Id"].str.lower().isin({"is20"})].copy()

    # intersection by numeric-clean IDs (this drives your 146/142 sizes)
    zoho_ids = digits_only_series(z["Merchant Number"])
    tsys_ids = digits_only_series(kept_tsys["Merchant ID"])
    fiserv_ids = digits_only_series(kept_fiserv["Merchant #"])
    valid = pd.Index(tsys_ids.dropna().unique()).union(pd.Index(fiserv_ids.dropna().unique()))
    z = z.loc[zoho_ids.isin(valid)].copy()

    # remove IS20-IS24
    agents_to_remove = {"IS20", "IS21", "IS22", "IS23", "IS24"}
    z = z.loc[~z["Sales Id"].isin(agents_to_remove)].copy()

    # output formatting
    z["Recurring Fee Code"] = 2
    z["Step 1"] = ""

    if "Annual PCI Fee Month to Charge" in z.columns:
        z = z.rename(columns={"Annual PCI Fee Month to Charge": "Recurring Fee Month"})
    if "Monthly Minimum MPA" in z.columns:
        z = z.rename(columns={"Monthly Minimum MPA": "Monthly Minimum"})

    z["Recurring Fee Month"] = pd.to_numeric(z.get("Recurring Fee Month", 0), errors="coerce").fillna(0).astype(int)
    selected_month = selected_month_year.month
    z["PCI Count"] = (z["Recurring Fee Month"].astype(str).str.strip() == str(selected_month)).astype(int)

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
    z = z.loc[:, final_cols].copy()

    z["Date Approved"] = pd.to_datetime(z["Date Approved"], errors="coerce").dt.strftime("%m/%d/%Y")
    z["Date Closed"] = pd.to_datetime(z["Date Closed"], errors="coerce").dt.strftime("%m/%d/%Y")

    proc = z["Processor"].fillna("").astype(str).str.strip().str.lower()
    z_fiserv = z.loc[proc.eq("fiserv")].copy()
    z_tsys = z.loc[proc.eq("tsys")].copy()

    return z_fiserv, z_tsys, final_cols


# =============================
# MEX functions (your MEX is already matching)
# =============================
def mex_for_monthly(mex_raw: pd.DataFrame) -> pd.DataFrame:
    m = mex_raw.copy()
    m["sales_rep_number"] = m["sales_rep_number"].astype(str).str.strip()
    m = m.loc[m["merchant_status"].fillna("").astype(str).str.strip().str.lower() != "c"].copy()
    reps = {"HUBW-0000000006", "HUBW-0000000124", "HUBW-0000000024"}
    m = m.loc[~m["sales_rep_number"].isin(reps)].copy()
    return m


def apply_mex_step1_lookup(zoho_keep_tsys: pd.DataFrame, kept_mex: pd.DataFrame, final_cols: list[str]) -> pd.DataFrame:
    mex_cols = [
        "visa_base_rate_discount_rev",
        "mc_base_rate_discount_rev",
        "disc_base_rate_discount_rev",
        "amex_base_rate_discount_rev",
    ]
    m = kept_mex.copy()
    m[mex_cols] = m[mex_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    m["merchant_id_clean"] = digits_only_series(m["merchant_id"])
    m["mex_step1"] = m[mex_cols].sum(axis=1)
    lookup = m.groupby("merchant_id_clean")["mex_step1"].sum()

    z = zoho_keep_tsys.copy()
    # IMPORTANT: clean Zoho merchant number for lookup
    z["Merchant_clean"] = digits_only_series(z["Merchant Number"])
    z["Step 1"] = z["Merchant_clean"].map(lookup).fillna(0)
    z = z.drop(columns=["Merchant_clean"], errors="ignore")
    return z.loc[:, final_cols].copy()


def mex_output_csv(mex_raw: pd.DataFrame, six_months_before: pd.Timestamp) -> pd.DataFrame:
    MEX = mex_raw.copy()
    MEX["sales_rep_number"] = MEX["sales_rep_number"].astype(str).str.strip()

    status_c = MEX["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
    removed_mex = MEX.loc[status_c].copy()
    kept_mex_1 = MEX.loc[~status_c].copy()

    cols_to_check = ["total_settle_tickets", "net_settle_volume", "merchant_total_revenue", "STW_total_residual"]
    for c in cols_to_check:
        if c in removed_mex.columns:
            removed_mex[c] = pd.to_numeric(removed_mex[c], errors="coerce")

    last_dep = pd.to_datetime(removed_mex.get("last_deposit_date"), errors="coerce")
    mask_back = (
        removed_mex["merchant_status"].fillna("").astype(str).str.strip().str.lower().eq("c")
        & (last_dep < six_months_before)
        & (removed_mex[cols_to_check].fillna(0).ne(0).any(axis=1))
    )

    to_keep = removed_mex.loc[mask_back].copy()
    kept_mex_1 = pd.concat([kept_mex_1, to_keep], ignore_index=True)

    sales_rep_numbers = ["HUBW-0000000006", "HUBW-0000000124"]
    kept_mex_1 = kept_mex_1.loc[~kept_mex_1["sales_rep_number"].isin(sales_rep_numbers)].copy()
    return kept_mex_1


# =============================
# Wireless count sheet (keep your earlier logic)
# =============================
def build_wireless_count_sheet(wcv_raw: pd.DataFrame) -> pd.DataFrame:
    WCV = wcv_raw.copy()
    WCV.rename(columns={WCV.columns[0]: "Mer + wir"}, inplace=True)
    if len(WCV.columns) >= 6:
        WCV.rename(columns={WCV.columns[5]: "Merchant Number"}, inplace=True)

    A = WCV["Mer + wir"].astype("string")
    mid_A = A.str.extract(r"(\d+)")[0]
    cnt_A = A.str.extract(r"\(\s*([^)]+)\s*\)")[0]

    lookup = pd.Series(cnt_A.values, index=mid_A).dropna()
    lookup = lookup[~lookup.index.duplicated(keep="first")]

    mid_F = WCV["Merchant Number"].astype("string").str.extract(r"(\d+)")[0]
    wireless_count = mid_F.map(lookup)

    acct_col = "Account Name" if "Account Name" in WCV.columns else WCV.columns[1]
    result = (
        pd.DataFrame(
            {
                "Merchant Number": mid_F,
                "Account Name": WCV[acct_col].astype("string"),
                "Wireless Count": wireless_count.astype("string"),
            }
        )
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .reset_index(drop=True)
    )
    return result


# =============================
# Valor processing (match your Original output characteristics)
# - Robust MID conversion (fix scientific notation)
# - Conditional 39 prefix for TSYS
# - Keep allowed-merchants expansion (helps match)
# - Add blank column " " at end
# =============================
def process_valor(valor_raw: pd.DataFrame, wireless_result: pd.DataFrame, kept_fiserv: pd.DataFrame, kept_tsys: pd.DataFrame) -> pd.DataFrame:
    Valor = valor_raw.copy()

    # normalize MID columns
    Valor["MID1"] = Valor.get("MID1", "").apply(excel_mid_to_string)
    Valor["MID2"] = Valor.get("MID2", "").apply(excel_mid_to_string)

    Valor["PROCESSOR"] = Valor.get("PROCESSOR", "").fillna("").astype(str).str.strip()
    Valor["DBA NAME"] = Valor.get("DBA NAME", "").fillna("").astype(str).str.strip()

    # Processor name in lowercase for logic only
    proc = Valor["PROCESSOR"].fillna("").astype(str).str.lower()
    cond_tsys = proc.str.startswith("tsys")

    # Conditional "39" prefix (do NOT double-prefix)
    mask_mid1 = cond_tsys & Valor["MID1"].ne("") & ~Valor["MID1"].astype(str).str.startswith("39")
    Valor.loc[mask_mid1, "MID1"] = "39" + Valor.loc[mask_mid1, "MID1"].astype(str)

    mask_mid2 = cond_tsys & Valor["MID1"].eq("") & Valor["MID2"].ne("") & ~Valor["MID2"].astype(str).str.startswith("39")
    Valor.loc[mask_mid2, "MID2"] = "39" + Valor.loc[mask_mid2, "MID2"].astype(str)

    # remove webb/mailbox plus
    dba_norm = Valor["DBA NAME"].fillna("").astype(str).str.strip().str.lower()
    mask_webb = (dba_norm.str.startswith("webb")) | (dba_norm == "mailbox plus")
    Valor = Valor.loc[~mask_webb].copy()

    # Allowed merchants from kept lists
    kept_f_ids = digits_only_series(kept_fiserv["Merchant #"])
    kept_t_ids = digits_only_series(kept_tsys["Merchant ID"])

    allowed = set(kept_f_ids) | set(kept_t_ids)
    # Expand TSYS variants (helps matching)
    allowed |= {("39" + x) for x in kept_t_ids if x and not str(x).startswith("39")}
    allowed.discard("")

    keep = Valor["MID1"].isin(allowed) | Valor["MID2"].isin(allowed)
    Valor = Valor.loc[keep].copy()

    # Wireless lookup (match)
    wireless_lookup = (
        wireless_result[["Merchant Number", "Wireless Count"]]
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .set_index("Merchant Number")["Wireless Count"]
    )

    # Column L is index 11; extract digits and map
    if len(Valor.columns) > 11:
        L_col = Valor.columns[11]
        Valor["_L_clean"] = Valor[L_col].astype("string").str.extract(r"(\d+)")[0]
        Valor["Wireless count"] = Valor["_L_clean"].map(wireless_lookup)

        aj_pos = 35
        col = Valor.pop("Wireless count")
        Valor.insert(min(aj_pos, len(Valor.columns)), "Wireless count", col)
        Valor.drop(columns=["_L_clean"], inplace=True, errors="ignore")

    # EXACT: add a blank column named single space at end
    Valor[" "] = ""

    return Valor


# =============================
# Step-1 pipeline
# =============================
def run_step1_pipeline(files: dict) -> dict[str, bytes]:
    outputs: dict[str, bytes] = {}

    # TSYS + Fiserv
    tsys_raw = read_csv_bytes(files["Synoptic_TSYS"])
    fiserv_raw = read_csv_bytes(files["Synoptic_Fiserv"], skiprows=1, dtype=str)

    kept_tsys = clean_tsys_synoptic(tsys_raw, selected_month_year, six_months_before)
    kept_fiserv = clean_fiserv_synoptic(fiserv_raw, selected_month_year)

    # PASO (this is where your mismatch was happening due to ID formatting)
    paso_s1 = read_csv_bytes(files["PASO_S1"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_s2 = read_csv_bytes(files["PASO_S2"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_kept = paso_output(paso_s1, paso_s2, kept_fiserv)
    outputs["PASO_Output.csv"] = to_csv_bytes(paso_kept)

    # Zoho (Monthly min workbook)
    zoho_raw = read_excel_bytes(
        files["Zoho_All_Fees"],
        skiprows=6,
        dtype={"Merchant Number": "string", "Sales Id": "string"},
    )
    zoho_keep_fiserv, zoho_keep_tsys, final_cols = process_zoho_for_monthly(
        zoho_raw, kept_tsys, kept_fiserv, selected_month_year
    )

    # MEX monthly + Step1 lookup for TSYS zoho
    mex_raw = read_excel_bytes(files["MEX_file"])
    kept_mex_monthly = mex_for_monthly(mex_raw)
    zoho_keep_tsys = apply_mex_step1_lookup(zoho_keep_tsys, kept_mex_monthly, final_cols)

    monthly_buf = io.BytesIO()
    with pd.ExcelWriter(monthly_buf, engine="openpyxl") as writer:
        zoho_keep_fiserv.to_excel(writer, sheet_name="Fiserv", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Step1", index=False)
        zoho_keep_tsys.to_excel(writer, sheet_name="TSYS", index=False)

        kept_mex_sheet = kept_mex_monthly.drop(columns=["merchant_id_clean", "mex_step1"], errors="ignore")
        kept_mex_sheet.to_excel(writer, sheet_name="MEX", index=False)

    monthly_buf.seek(0)
    outputs["Monthly min and annual PCI without Step1 Output.xlsx"] = monthly_buf.getvalue()

    # MEX output CSV (you already match)
    mex_out_df = mex_output_csv(mex_raw, six_months_before)
    outputs["MEX_Output.csv"] = to_csv_bytes(mex_out_df)

    # Wireless + Valor
    wireless_raw = read_excel_bytes(files["Zoho_Wireless"], skiprows=6)
    wireless_result = build_wireless_count_sheet(wireless_raw)

    valor_raw = read_excel_bytes(files["Valor"])
    valor_iso = process_valor(valor_raw, wireless_result, kept_fiserv, kept_tsys)

    valor_buf = io.BytesIO()
    with pd.ExcelWriter(valor_buf, engine="openpyxl") as writer:
        valor_iso.to_excel(writer, sheet_name="ISO Report", index=False)
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
# UI Uploaders
# =============================
st.header("📁 Step 1: Upload 8 Raw Input Files")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Synoptic")
    f_tsys = st.file_uploader("Synoptic — TSYS (CSV)", type=["csv"])
    f_fiserv = st.file_uploader("Synoptic — Fiserv (CSV)", type=["csv"])

    st.subheader("Zoho")
    f_zoho = st.file_uploader("Zoho — All Fees (XLSX)", type=["xlsx"])
    f_wireless = st.file_uploader("Zoho — Wireless (XLSX)", type=["xlsx"])

with col2:
    st.subheader("Other")
    f_mex = st.file_uploader("MEX file (XLSX)", type=["xlsx"])
    f_s1 = st.file_uploader("PASO S1 (CSV)", type=["csv"])
    f_s2 = st.file_uploader("PASO S2 (CSV)", type=["csv"])
    f_valor = st.file_uploader("Valor Step1 (XLSX)", type=["xlsx"])

files = {
    "Synoptic_TSYS": f_tsys,
    "Synoptic_Fiserv": f_fiserv,
    "Zoho_All_Fees": f_zoho,
    "Zoho_Wireless": f_wireless,
    "MEX_file": f_mex,
    "PASO_S1": f_s1,
    "PASO_S2": f_s2,
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
        with st.spinner("Running Step-1 pipeline..."):
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
# Download section
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
st.caption("Residuals Data Cleaning Pipeline — Step 1 • Matching mode")

