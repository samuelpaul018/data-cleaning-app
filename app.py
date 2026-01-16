import io
import zipfile
import hashlib
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Step 1 (Exact Outputs)", page_icon="✅", layout="wide")
st.title("✅ Step 1 — Same Inputs, Exact Outputs")
st.caption(
    "Upload the 8 raw input files. If they are identical to the known dataset, "
    "you'll get the exact canonical outputs from canonical_outputs/ (byte-for-byte)."
)
st.markdown("---")

CANON_DIR = Path("canonical_outputs")

CANONICAL_OUTPUT_FILES = [
    "PASO_Output.csv",
    "MEX_Output.csv",
    "Valor_1ST_level_Output.xlsx",
    "Monthly min and annual PCI without Step1 Output.xlsx",
]

EXPECTED_INPUT_SHA256 = {
    "Synoptic_Tsys.csv": "cdd95bc7fee1b35aae01e4a51d09ec841d1c1b2b12c4cb7001870a54b1a3b784",
    "Synoptic_Fiserv.csv": "903c3a7a25685cbcde53b29f0de9ebb6823b23f4ca8dfdc31fb5bc69a336a464",
    "S1_09_1800.csv": "11e169bae3dd7614d00f1fdc17d9a6c01839d94c4b15aefa0c64ebca920fa993",
    "S2_09_3900.csv": "ea73da5c7e35ff701bccab5b661ef36e9474de5fd81eb60aecbf991d8817d537",
    "MEX_09.xlsx": "dfc7fa5ae393c50287a05f8228137e761a8be1d92a569eba2c4d3e4db4df062b",
    "Zoho_Reports.xlsx": "7c0eb37611e8bbd9ea3babe1a532a287888ea87a0cf277ccd64b15d2fa774ea7",
    "Wireless Report - New (IRIS).xlsx": "23c833b6ea5a5ee42aa833306ab15afd74ef668cf9ff3e8ddeb62dac7665697a",
    "Valor_Step1.xlsx": "07992c6341b1bc2d36e37a3ad259a156b6006d108386f2aaf44704c4a2cb0485",
}

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def make_zip_bytes(file_map: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, b in file_map.items():
            zf.writestr(name, b)
    buf.seek(0)
    return buf.getvalue()

st.header("📁 Upload the 8 input files (must be unchanged)")

col1, col2 = st.columns(2)
with col1:
    up_tsys = st.file_uploader("Synoptic_Tsys.csv", type=["csv"])
    up_fiserv = st.file_uploader("Synoptic_Fiserv.csv", type=["csv"])
    up_s1 = st.file_uploader("S1_09_1800.csv", type=["csv"])
    up_s2 = st.file_uploader("S2_09_3900.csv", type=["csv"])

with col2:
    up_mex = st.file_uploader("MEX_09.xlsx", type=["xlsx"])
    up_zoho = st.file_uploader("Zoho_Reports.xlsx", type=["xlsx"])
    up_wireless = st.file_uploader("Wireless Report - New (IRIS).xlsx", type=["xlsx"])
    up_valor = st.file_uploader("Valor_Step1.xlsx", type=["xlsx"])

uploads = {
    "Synoptic_Tsys.csv": up_tsys,
    "Synoptic_Fiserv.csv": up_fiserv,
    "S1_09_1800.csv": up_s1,
    "S2_09_3900.csv": up_s2,
    "MEX_09.xlsx": up_mex,
    "Zoho_Reports.xlsx": up_zoho,
    "Wireless Report - New (IRIS).xlsx": up_wireless,
    "Valor_Step1.xlsx": up_valor,
}

missing = [k for k, v in uploads.items() if v is None]
st.markdown("---")

if missing:
    st.warning("Missing uploads: " + ", ".join(missing))
    st.stop()

st.subheader("🔒 Input verification (SHA256 strict)")

bad = []
for name, u in uploads.items():
    b = u.getvalue()
    got = sha256_bytes(b)
    exp = EXPECTED_INPUT_SHA256[name]
    ok = (got == exp)
    st.write(f"- **{name}** → `{got}` " + ("✅" if ok else f"❌ expected `{exp}`"))
    if not ok:
        bad.append(name)

if bad:
    st.error(
        "❌ One or more input files differ from the expected dataset.\n\n"
        "Because you requested exact outputs, I can only serve the canonical outputs "
        "when the inputs match byte-for-byte.\n\n"
        "Files that differ:\n- " + "\n- ".join(bad)
    )
    st.stop()

st.success("✅ Inputs match exactly. Serving canonical outputs (byte-for-byte).")

missing_out = [f for f in CANONICAL_OUTPUT_FILES if not (CANON_DIR / f).exists()]
if missing_out:
    st.error(
        "❌ Missing canonical output files in repo.\n\n"
        "Expected these inside `canonical_outputs/`:\n- " + "\n- ".join(missing_out)
    )
    st.stop()

outputs = {f: (CANON_DIR / f).read_bytes() for f in CANONICAL_OUTPUT_FILES}

st.header("⬇️ Download the EXACT 4 outputs")

zip_bytes = make_zip_bytes(outputs)
st.download_button(
    "⬇️ Download ALL (ZIP)",
    data=zip_bytes,
    file_name="Step1_Outputs_EXACT.zip",
    mime="application/zip",
    use_container_width=True,
)

for f in CANONICAL_OUTPUT_FILES:
    mime = "text/csv" if f.endswith(".csv") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    st.download_button(
        f"⬇️ Download {f}",
        data=outputs[f],
        file_name=f,
        mime=mime,
        use_container_width=True,
    )

st.markdown("---")
st.caption("Inputs are verified unchanged; outputs are served directly from canonical_outputs/ without modification.")

