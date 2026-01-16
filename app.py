import io
import zipfile
import streamlit as st

st.set_page_config(
    page_title="Step 1 Outputs (Exact)",
    page_icon="📦",
    layout="wide",
)

st.title("📦 Step 1 Outputs — EXACT (byte-for-byte)")
st.caption(
    "Upload the 4 Step-1 output files you want as the canonical truth. "
    "This app will return them for download with the standard filenames, "
    "preserving exact data + formatting."
)
st.markdown("---")

# =============================
# Session state
# =============================
if "outputs_bytes" not in st.session_state:
    st.session_state.outputs_bytes = {}
if "outputs_ready" not in st.session_state:
    st.session_state.outputs_ready = False

STANDARD_NAMES = {
    "PASO": "PASO_Output.csv",
    "MEX": "MEX_Output.csv",
    "MONTHLY": "Monthly min and annual PCI without Step1 Output.xlsx",
    "VALOR": "Valor_1ST_level_Output.xlsx",
}

def detect_key(filename: str) -> str | None:
    f = filename.lower()
    if "paso" in f and f.endswith(".csv"):
        return "PASO"
    if "mex_output" in f and f.endswith(".csv"):
        return "MEX"
    if "monthly min" in f and f.endswith(".xlsx"):
        return "MONTHLY"
    if "annual pci" in f and f.endswith(".xlsx"):
        return "MONTHLY"
    if "valor_1st_level_output" in f and f.endswith(".xlsx"):
        return "VALOR"
    if "valor" in f and "output" in f and f.endswith(".xlsx"):
        return "VALOR"
    return None

def make_zip(outputs: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, data in outputs.items():
            zf.writestr(fname, data)
    buf.seek(0)
    return buf.getvalue()

# =============================
# Upload UI
# =============================
st.header("1) Upload the 4 canonical output files")

c1, c2 = st.columns(2)
with c1:
    up1 = st.file_uploader("Upload file #1", type=["csv", "xlsx"], key="u1")
    up2 = st.file_uploader("Upload file #2", type=["csv", "xlsx"], key="u2")
with c2:
    up3 = st.file_uploader("Upload file #3", type=["csv", "xlsx"], key="u3")
    up4 = st.file_uploader("Upload file #4", type=["csv", "xlsx"], key="u4")

uploads = [u for u in [up1, up2, up3, up4] if u is not None]

st.markdown("---")

btns = st.columns([1, 1, 2])
with btns[0]:
    if st.button("🧹 Clear", use_container_width=True):
        st.session_state.outputs_bytes = {}
        st.session_state.outputs_ready = False
        st.rerun()

with btns[1]:
    build = st.button("✅ Use these as EXACT outputs", type="primary", use_container_width=True)

if build:
    outputs = {}
    unknown = []

    for u in uploads:
        key = detect_key(u.name)
        if key is None:
            unknown.append(u.name)
            continue
        outputs[STANDARD_NAMES[key]] = u.getvalue()

    # If some weren't detected but exactly 4 files uploaded, fallback: keep originals (still exact)
    if unknown and len(uploads) == 4 and len(outputs) < 4:
        # Add any undetected files under their original names
        for u in uploads:
            if detect_key(u.name) is None:
                outputs[u.name] = u.getvalue()

    st.session_state.outputs_bytes = outputs
    st.session_state.outputs_ready = len(outputs) >= 4

    if st.session_state.outputs_ready:
        st.success("✅ Stored. Downloads will be byte-for-byte identical to your uploads.")
    else:
        st.error(
            "❌ I couldn't detect all 4 standard outputs from filenames. "
            "Rename them to include PASO / MEX_Output / Monthly min / Valor_1ST_level_Output, then re-upload."
        )

# =============================
# Download section
# =============================
if st.session_state.outputs_bytes:
    st.header("2) Download (EXACT)")
    outputs = st.session_state.outputs_bytes

    # ZIP
    zip_bytes = make_zip(outputs)
    st.download_button(
        "⬇️ Download ALL outputs (ZIP)",
        data=zip_bytes,
        file_name="Step1_Outputs_EXACT.zip",
        mime="application/zip",
        use_container_width=True,
    )

    st.markdown("### Individual downloads")
    for fname in [
        STANDARD_NAMES["PASO"],
        STANDARD_NAMES["MEX"],
        STANDARD_NAMES["VALOR"],
        STANDARD_NAMES["MONTHLY"],
    ]:
        if fname not in outputs:
            continue
        mime = "text/csv" if fname.endswith(".csv") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        st.download_button(
            f"⬇️ {fname}",
            data=outputs[fname],
            file_name=fname,
            mime=mime,
            use_container_width=True,
        )

    st.markdown("---")
    st.caption("No regeneration. No pandas export. Your bytes are served back unchanged.")
else:
    st.info("Upload your 4 output files above, then click **Use these as EXACT outputs**.")
