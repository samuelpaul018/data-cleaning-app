import streamlit as st
import pandas as pd
import io
from github import Github
from datetime import datetime
import calendar
from pandas.tseries.offsets import MonthEnd, DateOffset

# Page configuration
st.set_page_config(
    page_title="Residuals Data Cleaning Pipeline",
    page_icon="🧹",
    layout="wide"
)

# Initialize session state
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'step1_files' not in st.session_state:
    st.session_state.step1_files = {}
if 'step2_files' not in st.session_state:
    st.session_state.step2_files = {}
if 'github_token' not in st.session_state:
    st.session_state.github_token = ""
if 'selected_month_year' not in st.session_state:
    st.session_state.selected_month_year = None
if 'step1_complete' not in st.session_state:
    st.session_state.step1_complete = False
if 'output_files' not in st.session_state:
    st.session_state.output_files = {}

# Header
st.title("🧹 Residuals Data Cleaning Pipeline")
st.markdown("---")

# Sidebar for GitHub configuration and Date Selection
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Date Selection
    st.subheader("📅 Select Month & Year")
    month_list = ["January", "February", "March", "April", "May", "June", 
                  "July", "August", "September", "October", "November", "December"]
    
    current_month_index = pd.Timestamp.today().month - 1
    selected_month = st.selectbox("Month:", month_list, index=current_month_index)
    selected_year = st.number_input("Year:", value=pd.Timestamp.today().year, min_value=2020, max_value=2030)
    
    # Calculate selected_month_year
    month_number = month_list.index(selected_month) + 1
    last_day = calendar.monthrange(selected_year, month_number)[1]
    st.session_state.selected_month_year = pd.Timestamp(f"{selected_year}-{month_number:02d}-{last_day}")
    
    st.info(f"Processing for: {st.session_state.selected_month_year.strftime('%B %Y')}")
    
    st.markdown("---")
    
    # GitHub Configuration
    st.subheader("🔗 GitHub Integration")
    github_token = st.text_input(
        "GitHub Personal Access Token",
        type="password",
        value=st.session_state.github_token,
        help="Enter your GitHub PAT to connect to repository"
    )
    if github_token:
        st.session_state.github_token = github_token
    
    repo_name = st.text_input(
        "Repository Name",
        placeholder="username/repo-name",
        help="Format: username/repository-name"
    )
    
    st.markdown("---")
    st.info("**Current Step:** " + str(st.session_state.step))

# Function to upload to GitHub
def upload_to_github(file_content, filename, token, repo_name):
    try:
        g = Github(token)
        repo = g.get_repo(repo_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"outputs/{timestamp}/{filename}"
        repo.create_file(file_path, f"Upload {filename}", file_content, branch="main")
        return True, file_path
    except Exception as e:
        return False, str(e)

# Data cleaning functions from your notebook
def clean_tsys_data(synoptic_tsys, selected_month_year):
    """Clean TSYS Synoptic data"""
    cols = ["Date Opened", "Date Closed", "Last Deposit Date"]
    synoptic_tsys[cols] = synoptic_tsys[cols].apply(pd.to_datetime, errors="coerce")
    
    six_months_before = selected_month_year - pd.DateOffset(months=6)
    
    # Remove: Date Opened > selected month
    mask_remove = (synoptic_tsys["Date Opened"] > selected_month_year)
    removed_tsys = synoptic_tsys.loc[mask_remove].copy()
    kept_tsys = synoptic_tsys.loc[~mask_remove].copy()
    
    # Reopen closed accounts with Date Closed > selected month
    mask_reopen = (
        kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed") &
        (kept_tsys["Date Closed"] > selected_month_year)
    )
    kept_tsys.loc[mask_reopen, "Status"] = "Open"
    
    # Remove closed with old/missing deposit
    status_closed = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().eq("closed")
    mask_no_deposit = kept_tsys["Last Deposit Date"].isna()
    mask_old_deposit = kept_tsys["Last Deposit Date"] <= six_months_before
    mask_remove_2 = status_closed & (mask_no_deposit | mask_old_deposit)
    
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_2]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_2].copy()
    
    # Remove by status
    statuses_to_remove = {"closed", "declined", "cancelled"}
    mask_remove_3 = kept_tsys["Status"].fillna("").astype(str).str.strip().str.lower().isin(statuses_to_remove)
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_remove_3]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_remove_3].copy()
    
    # Hard remove specific agents
    Agent_hard_remove = {"hubwallet", "stephany perez", "nigel westbury"}
    mask_hard_remove = kept_tsys["Rep Name"].fillna("").astype(str).str.strip().str.lower().isin(Agent_hard_remove)
    removed_tsys = pd.concat([removed_tsys, kept_tsys.loc[mask_hard_remove]], ignore_index=False)
    kept_tsys = kept_tsys.loc[~mask_hard_remove].copy()
    
    kept_tsys = kept_tsys.drop_duplicates(subset=["Merchant ID"], keep="first").copy()
    
    return kept_tsys, removed_tsys

def clean_fiserv_data(synoptic_fiserv, paso_s1, paso_s2, selected_month_year):
    """Clean Fiserv Synoptic data"""
    synoptic_fiserv = synoptic_fiserv.drop_duplicates(subset="Merchant #")
    
    cols = ["Open Date", "Close Date", "Last Batch Activity"]
    synoptic_fiserv[cols] = synoptic_fiserv[cols].apply(pd.to_datetime, errors="coerce")
    
    # Remove: Open Date > selected month
    mask_remove = (synoptic_fiserv["Open Date"] > selected_month_year)
    removed_fiserv = synoptic_fiserv.loc[mask_remove].copy()
    kept_fiserv = synoptic_fiserv.loc[~mask_remove].copy()
    
    # Reopen accounts
    mask_reopen = (
        kept_fiserv["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close") &
        (kept_fiserv["Close Date"] > selected_month_year)
    )
    kept_fiserv.loc[mask_reopen, "Merchant Status"] = "Open"
    
    six_months_before = (selected_month_year - pd.DateOffset(months=6)) + MonthEnd(0)
    
    # Remove closed with old batch
    status_close = kept_fiserv["Merchant Status"].fillna("").astype(str).str.strip().str.lower().eq("close")
    mask_no_batch = kept_fiserv["Last Batch Activity"].isna()
    mask_old_batch = kept_fiserv["Last Batch Activity"] <= six_months_before
    mask_remove_2 = status_close & (mask_no_batch | mask_old_batch)
    
    removed_fiserv = pd.concat([removed_fiserv, kept_fiserv.loc[mask_remove_2]], ignore_index=False)
    kept_fiserv = kept_fiserv.loc[~mask_remove_2].copy()
    
    # Agent filtering
    Agent_to_keep = {"2030", "3030", "4030", "5030"}
    sa = kept_fiserv["Sales Agent"].fillna("").astype(str).str.strip()
    is_numeric = sa.str.isnumeric()
    mask_remove_numeric = is_numeric & (~sa.isin(Agent_to_keep))
    
    removed_fiserv = pd.concat([removed_fiserv, kept_fiserv.loc[mask_remove_numeric]], ignore_index=False)
    kept_fiserv = kept_fiserv.loc[~mask_remove_numeric].copy()
    
    # Hard remove
    Agent_hard_remove = {"IS02"}
    mask_hard_remove = kept_fiserv["Sales Agent"].fillna("").astype(str).str.strip().isin(Agent_hard_remove)
    removed_fiserv = pd.concat([removed_fiserv, kept_fiserv.loc[mask_hard_remove]], ignore_index=False)
    kept_fiserv = kept_fiserv.loc[~mask_hard_remove].copy()
    
    kept_fiserv = kept_fiserv.drop_duplicates(subset=["Merchant #"], keep="first").copy()
    
    # PASO comparison
    PASO = pd.concat([paso_s1, paso_s2], ignore_index=True)
    paso_merchants = PASO["MerchantNumber"].dropna().astype(str).str.strip().unique()
    mask_back_to_kept = removed_fiserv["Merchant #"].fillna("").astype(str).str.strip().isin(paso_merchants)
    
    kept_fiserv = pd.concat([kept_fiserv, removed_fiserv.loc[mask_back_to_kept]], ignore_index=False)
    removed_fiserv = removed_fiserv.loc[~mask_back_to_kept].copy()
    
    return kept_fiserv, removed_fiserv, PASO

def process_step1_files(files, selected_month_year):
    """Process all Step 1 files and generate outputs"""
    output_files = {}
    
    # Clean TSYS
    kept_tsys, removed_tsys = clean_tsys_data(files["Synoptic_TSYS"], selected_month_year)
    
    # Clean Fiserv (with PASO)
    kept_fiserv, removed_fiserv, PASO = clean_fiserv_data(
        files["Synoptic_Fiserv"], 
        files["PASO_S1"], 
        files["PASO_S2"],
        selected_month_year
    )
    
    # Clean Zoho - simplified version
    zoho = files["Zoho_All_Fees"]
    # Add your Zoho cleaning logic here
    
    # Clean MEX
    mex = files["MEX_file"]
    # Add your MEX cleaning logic here
    
    # Clean Valor
    valor = files["Valor"]
    # Add your Valor cleaning logic here
    
    # Save outputs
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        kept_tsys.to_excel(writer, sheet_name='TSYS_Kept', index=False)
        kept_fiserv.to_excel(writer, sheet_name='Fiserv_Kept', index=False)
        PASO.to_excel(writer, sheet_name='PASO', index=False)
    buffer.seek(0)
    output_files['Step1_Output.xlsx'] = buffer
    
    return output_files

# STEP 1
if st.session_state.step == 1:
    st.header("📁 Step 1: Upload Initial Files")
    st.markdown("Please upload all 8 required files:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Synoptic Files")
        synoptic_tsys = st.file_uploader("Synoptic — TSYS", key="synoptic_tsys")
        synoptic_fiserv = st.file_uploader("Synoptic — Fiserv", key="synoptic_fiserv")
        
        st.subheader("Zoho Files")
        zoho_fees = st.file_uploader("Zoho — All Fees", key="zoho_fees")
        zoho_wireless = st.file_uploader("Zoho — Wireless", key="zoho_wireless")
    
    with col2:
        st.subheader("PASO & Other Files")
        mex_file = st.file_uploader("MEX file", key="mex_file")
        paso_s1 = st.file_uploader("PASO S1", key="paso_s1")
        paso_s2 = st.file_uploader("PASO S2", key="paso_s2")
        valor = st.file_uploader("Valor", key="valor")
    
    # Check if all files uploaded
    all_files = [synoptic_tsys, synoptic_fiserv, zoho_fees, zoho_wireless, 
                 mex_file, paso_s1, paso_s2, valor]
    all_uploaded = all(f is not None for f in all_files)
    
    st.markdown("---")
    
    if all_uploaded:
        st.success("✅ All files uploaded successfully!")
        
        if not st.session_state.step1_complete:
            if st.button("🚀 Proceed to Process Step 1", type="primary", use_container_width=True):
                with st.spinner("Processing files..."):
                    try:
                        # Read files
                        files_dict = {
                            "Synoptic_TSYS": pd.read_csv(synoptic_tsys),
                            "Synoptic_Fiserv": pd.read_csv(synoptic_fiserv, skiprows=1),
                            "Zoho_All_Fees": pd.read_excel(zoho_fees, skiprows=6, engine='openpyxl'),
                            "Zoho_Wireless": pd.read_excel(zoho_wireless, skiprows=6, engine='openpyxl'),
                            "MEX_file": pd.read_excel(mex_file, engine='openpyxl'),
                            "PASO_S1": pd.read_csv(paso_s1, skiprows=1),
                            "PASO_S2": pd.read_csv(paso_s2),
                            "Valor": pd.read_excel(valor, engine='openpyxl')
                        }
                        
                        # Process
                        output_files = process_step1_files(files_dict, st.session_state.selected_month_year)
                        
                        # Save to session state
                        st.session_state.step1_complete = True
                        st.session_state.output_files = output_files
                        st.rerun()
                            
                    except Exception as e:
                        st.error(f"Error processing files: {str(e)}")
        
        # Show outputs and continue button if processing is complete
        if st.session_state.step1_complete:
            st.success("✅ Processing complete!")
            st.subheader("📥 Download Output Files")
            
            # Download buttons
            for filename, file_content in st.session_state.output_files.items():
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.download_button(
                        label=f"⬇️ Download {filename}",
                        data=file_content.getvalue(),
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"dl_{filename}"
                    )
                
                with col_b:
                    if st.session_state.github_token and repo_name:
                        if st.button(f"Upload", key=f"gh_{filename}"):
                            success, result = upload_to_github(
                                file_content.getvalue(),
                                filename,
                                st.session_state.github_token,
                                repo_name
                            )
                            if success:
                                st.success(f"✓")
                            else:
                                st.error(f"✗")
            
            st.markdown("---")
            if st.button("➡️ Continue to Step 2", type="primary", key="goto_step2", use_container_width=True):
                st.session_state.step = 2
                st.session_state.step1_complete = False  # Reset for next time
                st.rerun()
    else:
        missing_count = sum(1 for f in all_files if f is None)
        st.warning(f"⚠️ Please upload all files. {missing_count} file(s) remaining.")

# STEP 2
elif st.session_state.step == 2:
    st.header("📁 Step 2: Upload Additional Files")
    st.markdown("Upload the Monthly Min and Valor files from Step 1 processing:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        monthly_min = st.file_uploader("Monthly Min — Step 1", key="monthly_min")
    
    with col2:
        valor_step1 = st.file_uploader("Valor — Step 1", key="valor_step1")
    
    all_uploaded = monthly_min is not None and valor_step1 is not None
    
    st.markdown("---")
    
    if all_uploaded:
        st.success("✅ All files uploaded successfully!")
        
        if st.button("🚀 Proceed to Process Step 2", type="primary", use_container_width=True):
            with st.spinner("Processing Step 2..."):
                try:
                    # Add Step 2 processing logic here
                    st.success("✅ Step 2 processing complete!")
                    st.info("Final outputs ready for download")
                    
                    st.markdown("---")
                    if st.button("🔄 Start Over", type="secondary"):
                        st.session_state.step = 1
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Error in Step 2: {str(e)}")
    else:
        st.warning("⚠️ Please upload both files to continue.")
    
    if st.button("⬅️ Back to Step 1"):
        st.session_state.step = 1
        st.rerun()

st.markdown("---")
st.caption("Residuals Data Cleaning Pipeline v2.0")
