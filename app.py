# app.py
import re
from io import BytesIO
import numpy as np
import pandas as pd
import streamlit as st

# ------------------------ PAGE SETUP ------------------------
st.set_page_config(page_title="Duct-wise Shipping Summary", layout="wide")

# ------------------------ CONFIG ---------------------------
OUTPUT_DETAILED = "duct_summary_detailed.xlsx"
OUTPUT_MASTER   = "duct_master_sheet.xlsx"

REQUIRED_COLS = [
    "Vendor Name",
    "PO No",
    "Package Code of Description",
    "DU Code",
    "DU Description",
    "Total Weight",
    "Modified On",
    "Package Code of MR",
    "Mark No",  # used in master ordering
]

MASTER_FRONT_COLS = [
    "DU Code",
    "DU Description",
    "Mark No",
    "DU Quantity",
    "DU UOM",
    "DU Qty In Alternative UOM",
    "Alternate UOM",
    "Unit Weight in KG",
    "Total Weight",
    "Vendor Name",
    "PO No",
    "Package Code of Description",
    "Package Code of MR",
    "Mfg_DrawingNo",
    "Mfg_DrawingRevNo",
    "Shipping list rev. no(Alternate BOM)",
    "MR No",
    "Created On",
    "Modified On",
]

# ------------------------ REGEX (precompiled) --------------------------
# Updated to support endings like 637X01 / 637HX01 in addition to Pxx and NNN
RE_DUCT_NO   = re.compile(r"(\d{3})(?:P\d{2}|[A-Z]{1,3}\d{2,3}|\d{3})$")
RE_AB_CODE   = re.compile(r"\bAB\s*\d{3}\b")
RE_GB_CODE   = re.compile(r"\bGB\s*\d{3}\b")
RE_PANEL_TAG = re.compile(r"PANEL|PANELASSY", re.IGNORECASE)
RE_PANEL_NO  = re.compile(r"^P(\d+)$")
RE_NUMERIC   = re.compile(r"^\d+$")

# ------------------------ HELPERS --------------------------
def extract_duct_no(du_code: str):
    if not isinstance(du_code, str):
        return None
    core = du_code.split("(")[0].strip()
    core_u = core.upper()  # handle x01/hx01 etc.
    m = RE_DUCT_NO.search(core_u)
    return m.group(1) if m else None

def extract_unit_wbs(pkg_code: str):
    if not isinstance(pkg_code, str):
        return None, None
    s = pkg_code.strip().strip("-")
    parts = s.split("-")
    if len(parts) >= 2:
        return parts[0], f"{parts[0]}-{parts[1]}"
    elif len(parts) == 1 and parts[0]:
        return parts[0], None
    return None, None

def detect_duct_prefix_for_block(group_df: pd.DataFrame) -> str | None:
    if "DU Description" not in group_df.columns:
        return None
    U = group_df["DU Description"].dropna().astype(str).str.upper()
    if U.str.contains(RE_AB_CODE).any():
        return "AB"
    if U.str.contains(RE_GB_CODE).any():
        return "GB"
    is_panel = U.str.contains(RE_PANEL_TAG)
    if (is_panel & U.str.contains("AB")).any():
        return "AB"
    if (is_panel & U.str.contains("GB")).any():
        return "GB"
    return None

def build_duct_key_column(df: pd.DataFrame) -> pd.Series:
    if "Duct No" not in df.columns:
        return pd.Series([None] * len(df), index=df.index)
    prefix_by_duct = (
        df.groupby("Duct No", dropna=True, as_index=True)
          .apply(detect_duct_prefix_for_block)
    )
    prefix_series = df["Duct No"].map(prefix_by_duct)
    duct_str = df["Duct No"].astype(str)
    duct_key = np.where(prefix_series.notna(), prefix_series.fillna("") + duct_str, duct_str)
    return pd.Series(duct_key, index=df.index)

def format_date_cols(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            parsed = pd.to_datetime(df[c], errors="coerce")
            if parsed.notna().sum() > 0:
                df[c] = parsed.dt.strftime("%d-%b-%Y")
    return df

@st.cache_data(show_spinner=False)
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return buf.getvalue()

@st.cache_data(show_spinner=False)
def load_uploaded_excels(files):
    """Read all uploaded .xlsx files (all sheets) into a single DF."""
    all_rows, errors = [], []
    for uf in files:
        try:
            xls = pd.read_excel(uf, sheet_name=None, dtype=str)
            for _, _df in xls.items():
                if _df is not None and not _df.empty:
                    all_rows.append(_df)
        except Exception as e:
            errors.append((getattr(uf, "name", "uploaded.xlsx"), str(e)))

    if not all_rows:
        return pd.DataFrame(), errors

    big = pd.concat(all_rows, ignore_index=True)
    big.columns = [c.strip() for c in big.columns]

    missing = [c for c in REQUIRED_COLS if c not in big.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    big["Duct No"] = big["DU Code"].apply(extract_duct_no)
    big["Unit No"], big["WBS Code"] = zip(*big["Package Code of MR"].apply(extract_unit_wbs))
    big["Duct Key"] = build_duct_key_column(big)

    if "Total Weight" in big.columns:
        big["Total Weight"] = pd.to_numeric(big["Total Weight"], errors="coerce").fillna(0)
    if "Modified On" in big.columns:
        big["Modified On"] = pd.to_datetime(big["Modified On"], errors="coerce")

    return big, errors


def summarize_by_duct_vendor_po(df: pd.DataFrame) -> pd.DataFrame:
    filt = df[df["Duct No"].notna() & df["Duct Key"].notna()].copy()
    agg = (
        filt.groupby(
            ["Duct Key", "Unit No", "WBS Code", "Vendor Name", "PO No", "Package Code of Description"],
            as_index=False, dropna=False
        ).agg(
            Total_Weight=("Total Weight", "sum"),
            Latest_Modified_On=("Modified On", "max"),
        )
        .sort_values(by=["Duct Key", "Unit No", "Vendor Name", "PO No"])
        .reset_index(drop=True)
    )
    return agg.rename(columns={"Duct Key": "Duct No"})

def build_master_sheet(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d = d[d.get("Duct Key").notna() & d.get("Duct No").notna()].copy()

    # Panel/mark ordering (vectorized)
    mn = d["Mark No"].astype(str).str.strip().str.upper()
    panel_num = mn.str.extract(RE_PANEL_NO)[0].astype("Int64")
    is_panel = panel_num.notna()
    is_numeric = mn.str.match(RE_NUMERIC)

    d["__grp__"]  = np.select([is_panel, is_numeric], [0, 1], default=2)
    d["__ord1__"] = panel_num.fillna(10**9)
    d["__ord2__"] = pd.to_numeric(mn.where(is_numeric), errors="coerce").fillna(10**9)

    d = d.sort_values(
        by=["Duct Key", "Unit No", "__grp__", "__ord1__", "__ord2__", "PO No", "DU Code"],
        na_position="last"
    ).drop(columns=["__grp__", "__ord1__", "__ord2__"], errors="ignore")

    existing_front = [c for c in MASTER_FRONT_COLS if c in d.columns]
    out = d[["Duct Key", "Unit No", "WBS Code"] + existing_front].copy()
    out = out.rename(columns={"Duct Key": "Duct No"})

    out = format_date_cols(out, ["Created On", "Modified On"])
    return out

# ------------------------ UI ------------------------------
st.title("")
st.caption("Upload one or more Excel files (.xlsx). We’ll generate: 1) Duct-wise Summary, 2) Master Sheet.")

if st.sidebar.button("🔄 Clear cache"):
    st.cache_data.clear()
    st.rerun()

uploaded_files = st.file_uploader(
    "Upload shipping list Excel file(s)",
    type=["xlsx"],
    accept_multiple_files=True
)

if not uploaded_files:
    st.info("Please upload at least one .xlsx file to proceed.")
    st.stop()

with st.spinner("Reading and processing uploaded files..."):
    try:
        big, errors = load_uploaded_excels(uploaded_files)
    except ValueError as ve:
        st.error(str(ve))
        st.stop()

if errors:
    with st.sidebar.expander("Files Skipped / Errors", expanded=False):
        for fname, e in errors:
            st.write(f"**{fname}** — {e}")

if big.empty:
    st.warning("No usable data found in the uploaded files.")
    st.stop()

# ----------------- FILTERS: Unit → Vendor -----------------
units = sorted([u for u in big["Unit No"].dropna().unique()])
if units:
    unit_sel = st.sidebar.multiselect("Filter by Unit No", options=units, default=units)
    tmp = big[big["Unit No"].isin(unit_sel)]
else:
    st.sidebar.info("No Unit No found; showing all data.")
    tmp = big

vendors = sorted([v for v in tmp["Vendor Name"].dropna().unique()])
vendor_sel = st.sidebar.multiselect("Then by Vendor Name", options=vendors, default=vendors) if vendors else []
df = tmp[tmp["Vendor Name"].isin(vendor_sel)] if vendors else tmp

st.sidebar.info(f"Filtered rows: **{len(df):,}**")
if df.empty:
    st.warning("No data after filters. Adjust Unit/Vendor selections.")
    st.stop()

with st.expander("Preview: Filtered Rows (first 200)", expanded=False):
    st.dataframe(df.head(200), use_container_width=True)

# ----------------- DETAIL VIEW (Drilldown) --------------------
st.subheader("Duct-wise Summary: Duct × Unit × Vendor × PO")
detailed = summarize_by_duct_vendor_po(df).rename(
    columns={
        "Total_Weight": "Total Weight",
        "Latest_Modified_On": "Latest Modified On",
    }
)
det_cols = [
    "Duct No", "Unit No", "WBS Code",
    "Vendor Name", "PO No", "Package Code of Description",
    "Total Weight", "Latest Modified On",
]
st.dataframe(detailed[det_cols], use_container_width=True)

st.download_button(
    "⬇️ Download duct_summary_detailed.xlsx",
    data=to_excel_bytes(detailed[det_cols]),
    file_name=OUTPUT_DETAILED,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="download_duct_summary_detailed",
)

# ----------------- MASTER SHEET -----------------------------
st.subheader("Master Sheet: All DU Items per Duct × Unit")
master_df = build_master_sheet(df)
st.dataframe(master_df.head(1000), use_container_width=True)

st.download_button(
    "⬇️ Download duct_master_sheet.xlsx",
    data=to_excel_bytes(master_df),
    file_name=OUTPUT_MASTER,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="download_duct_master_sheet",
)

st.success("Ready. Upload → Filter (Unit, Vendor) → Download the two outputs.")
