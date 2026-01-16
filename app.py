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

st.title("🧹 Residuals Data Cleaning Pipeline — Step 1 (Final.ipynb aligned)")
st.caption(
    "Generates: PASO_Output.csv, MEX_Output.csv, Valor_1ST_level_Output.xlsx, "
    "Monthly min and annual PCI without Step1 Output.xlsx"
)
st.markdown("---")

# =============================
# Session state (prevents reset on rerun/download)
# =============================
if "step1_outputs" not in st.session_state:
    # dict[str, bytes]
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


def clean_id_numeric(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)
    s = s.str.replace(r"\D+", "", regex=True)
    s = s.replace("", pd.NA)
    return s


# =============================
# TSYS synoptic cleaning (Final.ipynb logic)
# =============================
def clean_tsys_synoptic(tsys_df: pd.DataFrame, selected_month_year: pd.Timestamp, six_months_before: pd.Timestamp) -> pd.DataFrame:
    df = tsys_df.copy()

    for c in ["Date Opened", "Date Closed", "Last Deposit Date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "Date Opened" in df.columns:
        df = df.loc[~(df["Date Opened"] > selected_month_year)].copy()

    if "Status" in df.columns and "Date Closed" in df.columns:
        mask = (df["Status"].astype(str).str.strip().str.lower() == "closed") & (df["Date Closed"] > selected_month_year)
        df.loc[mask, "Status"] = "Open"

    if "Status" in df.columns and "Last Deposit Date" in df.columns:
        status_closed = df["Status"].astype(str).str.strip().str.lower().eq("closed")
        mask_remove = status_closed & (df["Last Deposit Date"].isna() | (df["Last Deposit Date"] <= six_months_before))
        df = df.loc[~mask_remove].copy()

    if "Status" in df.columns:
        rm = {"closed", "declined", "cancelled"}
        df = df.loc[~df["Status"].astype(str).str.strip().str.lower().isin(rm)].copy()

    if "Rep Name" in df.columns:
        hard = {"hubwallet", "stephany perez", "nigel westbury", "brandon casillas"}
        df = df.loc[~df["Rep Name"].astype(str).str.strip().str.lower().isin(hard)].copy()

    if "Merchant ID" in df.columns:
        df = df.drop_duplicates(subset=["Merchant ID"], keep="first")

    return df


# =============================
# Fiserv synoptic cleaning (Final.ipynb logic)
# =============================
def clean_fiserv_synoptic(
    fiserv_df: pd.DataFrame,
    selected_month_year: pd.Timestamp,
    six_months_before: pd.Timestamp,
    paso_all: pd.DataFrame
) -> pd.DataFrame:
    df = fiserv_df.copy()
    df = clean_nbsp(df)

    if "Merchant #" in df.columns:
        df = df.drop_duplicates(subset=["Merchant #"])

    for c in ["Open Date", "Close Date", "Last Batch Activity"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "Open Date" in df.columns:
        df = df.loc[~(df["Open Date"] > selected_month_year)].copy()

    # IMPORTANT: detect "close", "closed", "close " etc.
    def is_close(series: pd.Series) -> pd.Series:
        return (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.startswith("close")
        )

    if "Merchant Status" in df.columns and "Close Date" in df.columns:
        status_close = is_close(df["Merchant Status"])
        mask = status_close & (df["Close Date"] > selected_month_year)
        df.loc[mask, "Merchant Status"] = "Open"

    # Sales Agent rule
    if "Sales Agent" in df.columns:
        agent_keep = {"2030", "3030", "4030", "5030"}
        sa = df["Sales Agent"].fillna("").astype(str).str.strip()

        mask_numeric = sa.str.fullmatch(r"\d+", na=False)
        mask_has_letter = sa.str.contains(r"[A-Za-z]", regex=True, na=False)
        mask_numeric_keep = mask_numeric & sa.isin(agent_keep)

        keep_mask = mask_has_letter | mask_numeric_keep
        df = df.loc[keep_mask].copy()

        df = df.loc[~df["Sales Agent"].fillna("").astype(str).str.strip().isin({"IS02"})].copy()

    # Merchant # digits only
    if "Merchant #" in df.columns:
        df["Merchant #"] = (
            df["Merchant #"].fillna("").astype(str).str.strip()
            .str.encode("ascii", "ignore").str.decode("ascii")
            .str.replace(r"\D+", "", regex=True)
        )

    # ✅ PASO fix: remove CLOSE* merchants with old/blank Last Batch Activity (Final.ipynb behavior)
    if "Merchant Status" in df.columns and "Last Batch Activity" in df.columns:
        status_close = is_close(df["Merchant Status"])
        lba = pd.to_datetime(df["Last Batch Activity"], errors="coerce")
        df = df.loc[~(status_close & (lba.isna() | (lba <= six_months_before)))].copy()

    # Closed merchants: keep only those present in PASO list
    if "Merchant Status" in df.columns and "Merchant #" in df.columns:
        paso_merchants = paso_all["MerchantNumber"].dropna().astype(str).str.strip().unique()
        status_close = is_close(df["Merchant Status"])
        df = df.loc[(~status_close) | (df["Merchant #"].isin(paso_merchants))].copy()

    return df


# =============================
# Zoho processing (Final.ipynb logic)
# =============================
def process_zoho(
    zoho_raw: pd.DataFrame,
    kept_tsys: pd.DataFrame,
    kept_fiserv: pd.DataFrame,
    selected_month_year: pd.Timestamp
):
    z = zoho_raw.copy()

    for c in ["Date Approved", "Date Closed"]:
        if c in z.columns:
            z[c] = pd.to_datetime(z[c], errors="coerce")

    z["Sales Id"] = z.get("Sales Id", "").fillna("").astype("string").str.strip()
    z["Merchant Number"] = z.get("Merchant Number", "").fillna("").astype("string").str.strip()
    z["Account Status"] = z.get("Account Status", "").fillna("").astype("string").str.strip()

    mask_has_letter = z["Sales Id"].str.contains(r"[A-Za-z]", regex=True, na=False)
    z = z.loc[mask_has_letter].copy()

    if "Date Approved" in z.columns:
        z = z.loc[~(z["Date Approved"] > selected_month_year)].copy()

    if "Date Closed" in z.columns:
        mask = (z["Account Status"].str.lower() == "closed") & (z["Date Closed"] > selected_month_year)
        z.loc[mask, "Account Status"] = "Approved"

    statuses_to_remove = {"closed", "declined", "n/a", ""}
    z = z.loc[~z["Account Status"].str.lower().isin(statuses_to_remove)].copy()

    z = z.loc[~z["Sales Id"].str.lower().isin({"is20"})].copy()

    zoho_ids = clean_id_numeric(z["Merchant Number"])
    tsys_ids = clean_id_numeric(kept_tsys["Merchant ID"])
    fiserv_ids = clean_id_numeric(kept_fiserv["Merchant #"])
    valid = pd.Index(tsys_ids.dropna().unique()).union(pd.Index(fiserv_ids.dropna().unique()))
    z = z.loc[zoho_ids.isin(valid)].copy()

    agents_to_remove = {"IS20", "IS21", "IS22", "IS23", "IS24"}
    z = z.loc[~z["Sales Id"].isin(agents_to_remove)].copy()

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
# MEX logic
# =============================
def mex_for_monthly(mex_raw: pd.DataFrame) -> pd.DataFrame:
    m = mex_raw.copy()
    m["sales_rep_number"] = m["sales_rep_number"].astype(str).str.strip()

    keep = m["merchant_status"].fillna("").astype(str).str.strip().str.lower() != "c"
    m = m.loc[keep].copy()

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

    m["merchant_id_clean"] = m["merchant_id"].astype(str).str.strip()
    m["mex_step1"] = m[mex_cols].sum(axis=1)

    lookup = m.groupby("merchant_id_clean")["mex_step1"].sum()

    z = zoho_keep_tsys.copy()
    z["Merchant_clean"] = z["Merchant Number"].astype(str).str.strip()
    z["Step 1"] = z["Merchant_clean"].map(lookup).fillna(0)

    z = z.drop(columns=["Merchant_clean"], errors="ignore")
    z = z.loc[:, final_cols].copy()
    return z


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
    removed_mex = removed_mex.loc[~mask_back].copy()

    kept_mex_1 = pd.concat([kept_mex_1, to_keep], ignore_index=True)

    sales_rep_numbers = ["HUBW-0000000006", "HUBW-0000000124"]
    mask_remove_rep = kept_mex_1["sales_rep_number"].isin(sales_rep_numbers)
    kept_mex_1 = kept_mex_1.loc[~mask_remove_rep].copy()

    return kept_mex_1


# =============================
# Wireless count sheet
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
    result = pd.DataFrame(
        {
            "Merchant Number": mid_F,
            "Account Name": WCV[acct_col].astype("string"),
            "Wireless Count": wireless_count.astype("string"),
        }
    ).dropna(subset=["Merchant Number"]).drop_duplicates(subset=["Merchant Number"], keep="first").reset_index(drop=True)

    return result


# =============================
# Valor ISO report (with FIXES)
# =============================
def process_valor(valor_raw: pd.DataFrame, wireless_result: pd.DataFrame, kept_fiserv: pd.DataFrame, kept_tsys: pd.DataFrame) -> pd.DataFrame:
    Valor = valor_raw.copy()

    for col in ["MID1", "MID2", "PROCESSOR"]:
        if col in Valor.columns:
            Valor[col] = Valor[col].fillna("").astype(str).str.strip()

    Valor["MID1"] = Valor["MID1"].astype(str).str.replace(r"\.0+$", "", regex=True)
    Valor["MID2"] = Valor["MID2"].astype(str).str.replace(r"\.0+$", "", regex=True)

    Valor["Processor"] = Valor["PROCESSOR"].fillna("").astype(str).str.lower()

    # ✅ FIX: only add 39 if not already present
    cond_tsys = Valor["Processor"].str.startswith("tsys")

    mask_mid1 = cond_tsys & Valor["MID1"].ne("") & ~Valor["MID1"].astype(str).str.startswith("39")
    Valor.loc[mask_mid1, "MID1"] = "39" + Valor.loc[mask_mid1, "MID1"].astype(str)

    mask_mid2 = cond_tsys & Valor["MID1"].eq("") & Valor["MID2"].ne("") & ~Valor["MID2"].astype(str).str.startswith("39")
    Valor.loc[mask_mid2, "MID2"] = "39" + Valor.loc[mask_mid2, "MID2"].astype(str)

    dba_norm = Valor["DBA NAME"].fillna("").astype(str).str.strip().str.lower()
    mask_webb = (dba_norm.str.startswith("webb")) | (dba_norm == "mailbox plus")
    Valor = Valor.loc[~mask_webb].copy()

    kept_f_ids = kept_fiserv["Merchant #"].fillna("").astype(str).str.strip()
    kept_t_ids = kept_tsys["Merchant ID"].fillna("").astype(str).str.strip()

    allowed = set(kept_f_ids) | set(kept_t_ids)
    allowed |= {("39" + x) for x in kept_t_ids if x and not str(x).startswith("39")}
    allowed.discard("")

    Valor["MID1"] = Valor["MID1"].fillna("").astype(str).str.strip()
    Valor["MID2"] = Valor["MID2"].fillna("").astype(str).str.strip()

    keep = Valor["MID1"].isin(allowed) | Valor["MID2"].isin(allowed)
    Valor = Valor.loc[keep].copy()

    wireless_lookup = (
        wireless_result[["Merchant Number", "Wireless Count"]]
        .dropna(subset=["Merchant Number"])
        .drop_duplicates(subset=["Merchant Number"], keep="first")
        .set_index("Merchant Number")["Wireless Count"]
    )

    if len(Valor.columns) > 11:
        L_col_name = Valor.columns[11]
        Valor["_L_clean"] = Valor[L_col_name].astype("string").str.extract(r"(\d+)")[0]
        Valor["Wireless count"] = Valor["_L_clean"].map(wireless_lookup)

        aj_pos = 35
        col = Valor.pop("Wireless count")
        Valor.insert(min(aj_pos, len(Valor.columns)), "Wireless count", col)
        Valor.drop(columns=["_L_clean"], inplace=True, errors="ignore")

    Valor.drop(columns=["Processor"], inplace=True, errors="ignore")

    # ✅ FIX: match notebook extra blank column named exactly " "
    Valor[" "] = ""

    return Valor


# =============================
# Step-1 pipeline
# =============================
def run_step1_pipeline(files: dict) -> dict[str, bytes]:
    outputs: dict[str, bytes] = {}

    # Inputs
    tsys_raw = read_csv_bytes(files["Synoptic_TSYS"])
    fiserv_raw = read_csv_bytes(files["Synoptic_Fiserv"], skiprows=1, dtype=str)

    paso_s1 = read_csv_bytes(files["PASO_S1"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_s2 = read_csv_bytes(files["PASO_S2"], skiprows=1, dtype={"MerchantNumber": "string"})
    paso_all = pd.concat([clean_nbsp(paso_s1), clean_nbsp(paso_s2)], ignore_index=True)

    kept_tsys = clean_tsys_synoptic(tsys_raw, selected_month_year, six_months_before)
    kept_fiserv = clean_fiserv_synoptic(fiserv_raw, selected_month_year, six_months_before, paso_all)

    # PASO output
    kept_fiserv_mid = kept_fiserv["Merchant #"].astype("string").str.replace("\xa0", "", regex=False).str.strip()
    paso_all["MerchantNumber"] = paso_all["MerchantNumber"].astype("string").str.strip()
    paso_kept = paso_all.loc[paso_all["MerchantNumber"].isin(kept_fiserv_mid)].copy()
    outputs["PASO_Output.csv"] = to_csv_bytes(paso_kept)

    # Zoho
    zoho_raw = read_excel_bytes(
        files["Zoho_All_Fees"],
        skiprows=6,
        dtype={"Merchant Number": "string", "Sales Id": "string"},
    )
    zoho_keep_fiserv, zoho_keep_tsys, final_cols = process_zoho(zoho_raw, kept_tsys, kept_fiserv, selected_month_year)

    # MEX monthly + Step1 lookup for TSYS zoho
    mex_raw = read_excel_bytes(files["MEX_file"])
    kept_mex_monthly = mex_for_monthly(mex_raw)
    zoho_keep_tsys = apply_mex_step1_lookup(zoho_keep_tsys, kept_mex_monthly, final_cols)

    # Monthly min workbook
    monthly_buf = io.BytesIO()
    with pd.ExcelWriter(monthly_buf, engine="openpyxl") as writer:
        zoho_keep_fiserv.to_excel(writer, sheet_name="Fiserv", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Step1", index=False)
        zoho_keep_tsys.to_excel(writer, sheet_name="TSYS", index=False)

        kept_mex_sheet = kept_mex_monthly.drop(columns=["merchant_id_clean", "mex_step1"], errors="ignore")
        kept_mex_sheet.to_excel(writer, sheet_name="MEX", index=False)
    monthly_buf.seek(0)
    outputs["Monthly min and annual PCI without Step1 Output.xlsx"] = monthly_buf.getvalue()

    # MEX output CSV
    mex_out_df = mex_output_csv(mex_raw, six_months_before)
    outputs["MEX_Output.csv"] = to_csv_bytes(mex_out_df)

    # Wireless
    wireless_raw = read_excel_bytes(files["Zoho_Wireless"], skiprows=6)
    wireless_result = build_wireless_count_sheet(wireless_raw)

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
    valor_iso = process_valor(valor_raw, wireless_result, kept_fiserv, kept_tsys)

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

# Clear outputs button
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
# Download section (persists across reruns)
# =============================
if st.session_state.step1_ran and st.session_state.step1_outputs:
    st.markdown("---")
    st.subheader("📥 Download Outputs")

    outputs = st.session_state.step1_outputs

    # Download ALL at once
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
st.caption("Residuals Data Cleaning Pipeline — Step 1 (Final.ipynb aligned) • Persistent downloads + ZIP")

