import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from ai_investigator import (
    get_latest_quality_report,
    investigate_data_quality,
)


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
GOLD_DIR = DATA_DIR / "gold"
REPORTS_DIR = BASE_DIR / "reports"

st.set_page_config(
    page_title="DataSentinel AI",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# STYLING
# =========================================================

st.markdown(
    """
    <style>
    @import url(
        'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'
    );

    .stApp {
        background: #080D19;
        color: #F0F5FF;
        font-family: 'Inter', sans-serif;
    }

    [data-testid="stSidebar"] {
        background: #0C1322;
        border-right: 1px solid #24334A;
    }

    [data-testid="stSidebar"] * {
        color: #F0F5FF;
    }

    .block-container {
        padding-top: 2rem;
        max-width: 1500px;
    }

    h1, h2, h3 {
        color: #F0F5FF !important;
    }

    p {
        color: #A7B8D0;
    }

    .hero {
        padding: 30px;
        border-radius: 18px;
        border: 1px solid #244157;
        background:
            radial-gradient(
                circle at 90% 10%,
                rgba(67,217,232,0.15),
                transparent 35%
            ),
            linear-gradient(135deg, #111D30, #0D1423);
        margin-bottom: 25px;
    }

    .hero-title {
        font-size: 34px;
        font-weight: 800;
        color: #F0F5FF;
        margin-bottom: 8px;
    }

    .hero-subtitle {
        font-size: 14px;
        color: #A7B8D0;
        line-height: 1.7;
    }

    .eyebrow {
        color: #43D9E8;
        font-size: 11px;
        font-weight: 800;
        letter-spacing: 2px;
        margin-bottom: 10px;
    }

    .metric-card {
        background: linear-gradient(145deg, #121E30, #0E1727);
        border: 1px solid #24334A;
        border-radius: 15px;
        padding: 20px;
        min-height: 125px;
        margin-bottom: 10px;
    }

    .metric-label {
        color: #9AABC4;
        font-size: 12px;
        font-weight: 600;
        margin-bottom: 13px;
    }

    .metric-value {
        color: #F0F5FF;
        font-size: 27px;
        font-weight: 800;
        overflow-wrap: anywhere;
    }

    .metric-note {
        color: #43D9E8;
        font-size: 11px;
        margin-top: 9px;
    }

    .stage-card {
        background: #111C2D;
        border: 1px solid #263950;
        border-radius: 12px;
        padding: 17px;
        text-align: center;
        min-height: 100px;
        margin-bottom: 10px;
    }

    .stage-title {
        color: #43D9E8;
        font-weight: 700;
        font-size: 15px;
        margin-bottom: 8px;
    }

    .stage-description {
        color: #9AABC4;
        font-size: 12px;
    }

    
/* =========================================
   BUTTON STYLING — BLACK TEXT
   ========================================= */

/* Apply black text to all Streamlit buttons */
/* Default styling for all buttons: white text */
.stButton > button {
    color: white !important;
}

.stButton > button p,
.stButton > button span {
    color: white !important;
}

/* Black text for Run Pipeline and Run AI Investigation */
.stButton > button:has(p) {
    color: white;
}

/* Primary action buttons */
.stButton > button[kind="primary"] {
    color: black !important;
}

.stButton > button[kind="primary"] p,
.stButton > button[kind="primary"] span {
    color: black !important;
}
/* Keep existing button background colors; default text is white */
.stButton > button {
    color: white !important;
}

/* Make text black only on the two action buttons */
.stButton > button:has(p) {
    color: white;
}

/* Do not set background-color here */



    div[data-testid="stMetric"] {
        background: #111C2D;
        border: 1px solid #263950;
        padding: 16px;
        border-radius: 12px;
    }

    hr {
        border-color: #24334A;
    }

    .footer {
        text-align: center;
        color: #70829C;
        font-size: 11px;
        margin-top: 40px;
        padding-top: 20px;
        border-top: 1px solid #24334A;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def latest_directory(parent):
    if not parent.exists():
        return None

    directories = [
        item for item in parent.iterdir()
        if item.is_dir()
    ]

    return max(
        directories,
        key=lambda item: item.stat().st_mtime,
        default=None,
    )


def read_json(path):
    if not path or not Path(path).exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}


def read_parquet(path):
    if not path or not Path(path).exists():
        return pd.DataFrame()

    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def extract_records(data, key):
    """
    Support both:
      {"products": [...]}
    and:
      [...]
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        records = data.get(key, [])

        if isinstance(records, list):
            return records

    return []


def latest_report():
    if not REPORTS_DIR.exists():
        return None

    files = list(
        REPORTS_DIR.glob("data_quality_report_*.json")
    )

    return max(
        files,
        key=lambda item: item.stat().st_mtime,
        default=None,
    )


def latest_csv(pattern):
    if not REPORTS_DIR.exists():
        return None

    files = list(REPORTS_DIR.glob(pattern))

    return max(
        files,
        key=lambda item: item.stat().st_mtime,
        default=None,
    )


def find_value(data, keys, default=None):
    if not isinstance(data, dict):
        return default

    for key in keys:
        if key in data:
            return data[key]

    return default


def find_alerts(report):
    if not isinstance(report, dict):
        return []

    for key in (
        "alerts",
        "issues",
        "findings",
        "anomalies",
        "checks_failed",
    ):
        value = report.get(key)

        if isinstance(value, list):
            return value

    # Some reports store alerts inside nested dictionaries.
    for value in report.values():
        if isinstance(value, dict):
            result = find_alerts(value)

            if result:
                return result

    return []


def alert_to_text(alert):
    if isinstance(alert, dict):
        for key in (
            "message",
            "description",
            "issue",
            "name",
            "check_name",
            "alert",
        ):
            if alert.get(key):
                return str(alert[key])

        return json.dumps(alert, ensure_ascii=False)

    return str(alert)


def metric_card(label, value, note):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stage_card(title, description):
    st.markdown(
        f"""
        <div class="stage-card">
            <div class="stage-title">{title}</div>
            <div class="stage-description">{description}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_chart(fig):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Inter, sans-serif",
            color="#D8E4F5",
        ),
        margin=dict(l=10, r=10, t=50, b=10),
    )

    return fig


def download_json(data, filename):
    st.download_button(
        "Download JSON",
        data=json.dumps(data, indent=2, default=str),
        file_name=filename,
        mime="application/json",
        use_container_width=True,
    )


# =========================================================
# LOAD DATA
# =========================================================

bronze_run = latest_directory(BRONZE_DIR)
gold_run = latest_directory(GOLD_DIR)

quality_report_path = latest_report()
quality_report = read_json(quality_report_path)

products_path = (
    bronze_run / "products.json"
    if bronze_run else None
)

carts_path = (
    bronze_run / "carts.json"
    if bronze_run else None
)

products_data = read_json(products_path)
carts_data = read_json(carts_path)

# FIX: JSON files may contain lists or dictionaries.
products = extract_records(products_data, "products")
carts = extract_records(carts_data, "carts")

products_df = pd.DataFrame(products)
carts_df = pd.DataFrame(carts)

category_df = read_parquet(
    gold_run / "product_category_metrics.parquet"
    if gold_run else None
)

low_stock_df = read_parquet(
    gold_run / "low_stock_products.parquet"
    if gold_run else None
)

cart_metrics_df = read_parquet(
    gold_run / "cart_metrics.parquet"
    if gold_run else None
)

cart_distribution_df = read_parquet(
    gold_run / "cart_value_distribution.parquet"
    if gold_run else None
)

gold_summary = read_json(
    gold_run / "summary.json"
    if gold_run else None
)

alerts = find_alerts(quality_report)

quality_status = str(
    find_value(
        quality_report,
        ["status", "overall_status", "quality_status"],
        "NOT AVAILABLE",
    )
).upper()


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.markdown(
        """
        <div style="padding:12px 0 22px 0;">
            <div style="font-size:30px;">🛡️</div>
            <div style="font-size:23px;font-weight:800;">
                DataSentinel
            </div>
            <div style="
                font-size:11px;
                color:#43D9E8;
                letter-spacing:2px;
            ">
                AI DATA INTELLIGENCE
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    page = st.radio(
        "NAVIGATION",
        [
            "Overview",
            "Pipeline Monitor",
            "Business Analytics",
            "Data Quality",
            "AI Investigation",
            "Data Explorer",
        ],
    )

    st.markdown("---")
    st.markdown("### Pipeline Control")

    run_pipeline = st.button(
        "▶ Run Complete Pipeline",
        type="primary",
        use_container_width=True,
    )

    st.caption(
        "Ingest live API data, transform it, "
        "generate analytics and run quality checks."
    )

    st.markdown("---")
    st.markdown("### System Status")

    if bronze_run:
        st.success("Bronze data available")
    else:
        st.warning("Bronze data not found")

    if quality_report_path:
        st.success("Quality report available")
    else:
        st.warning("Quality report not found")


# =========================================================
# RUN PIPELINE
# =========================================================

if run_pipeline:

    with st.spinner("Running the complete data pipeline..."):

        start_time = time.time()

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(BASE_DIR / "pipeline.py"),
                ],
                cwd=str(BASE_DIR),
                capture_output=True,
                text=True,
                timeout=900,
            )

            st.session_state["pipeline_return_code"] = (
                result.returncode
            )

            st.session_state["pipeline_elapsed"] = (
                time.time() - start_time
            )

            st.session_state["pipeline_stdout"] = result.stdout
            st.session_state["pipeline_stderr"] = result.stderr

        except subprocess.TimeoutExpired:
            st.session_state["pipeline_return_code"] = -1
            st.session_state["pipeline_stderr"] = (
                "Pipeline exceeded the 15-minute timeout."
            )

        except Exception as error:
            st.session_state["pipeline_return_code"] = -1
            st.session_state["pipeline_stderr"] = str(error)

    st.rerun()


# =========================================================
# OVERVIEW
# =========================================================

if page == "Overview":

    st.markdown(
        """
        <div class="hero">
            <div class="eyebrow">
                INTELLIGENT DATA OPERATIONS PLATFORM
            </div>
            <div class="hero-title">
                DataSentinel AI
            </div>
            <div class="hero-subtitle">
                From live data ingestion to AI-assisted investigation.
                Monitor pipelines, explore business metrics and detect
                data-quality issues in one unified workspace.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        metric_card(
            "PRODUCTS INGESTED",
            f"{len(products):,}",
            "Live API records",
        )

    with c2:
        metric_card(
            "CARTS INGESTED",
            f"{len(carts):,}",
            "Transaction datasets",
        )

    with c3:
        metric_card(
            "QUALITY STATUS",
            quality_status,
            "Latest quality report",
        )

    with c4:
        metric_card(
            "REPORTED ALERTS",
            f"{len(alerts):,}",
            "Recognized alert entries",
        )

    st.markdown("## Data Pipeline Architecture")

    stages = st.columns(4)

    stage_definitions = [
        ("01 · BRONZE", "Live REST API ingestion"),
        ("02 · SILVER", "PySpark data transformation"),
        ("03 · GOLD", "Analytics-ready datasets"),
        ("04 · SENTINEL", "Quality checks and AI"),
    ]

    for column, (title, description) in zip(
        stages,
        stage_definitions,
    ):
        with column:
            stage_card(title, description)

    left, right = st.columns(2)

    with left:
        st.markdown("## Product Categories")

        if not category_df.empty:
            numeric_columns = list(
                category_df.select_dtypes(
                    include="number"
                ).columns
            )

            if numeric_columns:
                category_column = category_df.columns[0]

                fig = px.bar(
                    category_df,
                    x=category_column,
                    y=numeric_columns[0],
                    title="Category metrics",
                )

                st.plotly_chart(
                    style_chart(fig),
                    use_container_width=True,
                )
            else:
                st.dataframe(
                    category_df,
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info(
                "Run the pipeline to generate category metrics."
            )

    with right:
        st.markdown("## Recent Quality Alerts")

        if alerts:
            alert_preview = pd.DataFrame(
                [
                    {"Alert": alert_to_text(item)}
                    for item in alerts[:5]
                ]
            )

            st.dataframe(
                alert_preview,
                use_container_width=True,
                hide_index=True,
            )
        elif quality_report:
            st.info(
                "No standard alert list was found. "
                "Inspect the full report in Data Quality."
            )
        else:
            st.info("No quality report is available yet.")


# =========================================================
# PIPELINE MONITOR
# =========================================================

elif page == "Pipeline Monitor":

    st.title("Pipeline Monitor")

    st.caption(
        "Inspect recent execution details and generated artifacts."
    )

    result_code = st.session_state.get(
        "pipeline_return_code"
    )

    if result_code == 0:
        st.success("Last recorded pipeline run succeeded.")
    elif result_code is not None:
        st.error(
            f"Last recorded pipeline returned code {result_code}."
        )
    else:
        st.info(
            "No pipeline run has been recorded in this "
            "dashboard session."
        )

    elapsed = st.session_state.get("pipeline_elapsed")

    if elapsed is not None:
        st.metric(
            "Last execution duration",
            f"{elapsed:.2f} seconds",
        )

    st.markdown("### Pipeline stages")

    cols = st.columns(4)

    for column, title, description in zip(
        cols,
        ["Bronze", "Silver", "Gold", "Data Quality"],
        [
            "REST API ingestion",
            "PySpark transformations",
            "Business analytics",
            "Validation and reports",
        ],
    ):
        with column:
            stage_card(title, description)

    st.markdown("### Latest artifacts")

    artifacts = pd.DataFrame(
        [
            {
                "Artifact": "Bronze run",
                "Available": bool(bronze_run),
                "Path": str(bronze_run or "Not found"),
            },
            {
                "Artifact": "Gold run",
                "Available": bool(gold_run),
                "Path": str(gold_run or "Not found"),
            },
            {
                "Artifact": "Quality report",
                "Available": bool(quality_report_path),
                "Path": str(
                    quality_report_path or "Not found"
                ),
            },
        ]
    )

    st.dataframe(
        artifacts,
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Pipeline console output"):

        st.markdown("**Standard output**")

        st.code(
            st.session_state.get(
                "pipeline_stdout",
                "No output recorded.",
            )
        )

        st.markdown("**Error output**")

        st.code(
            st.session_state.get(
                "pipeline_stderr",
                "No error output recorded.",
            )
        )


# =========================================================
# BUSINESS ANALYTICS
# =========================================================

elif page == "Business Analytics":

    st.title("Business Analytics")

    st.caption(
        "Explore product categories, cart metrics and "
        "generated analytical datasets."
    )

    st.markdown("### Product category metrics")

    if not category_df.empty:

        numeric_columns = list(
            category_df.select_dtypes(
                include="number"
            ).columns
        )

        if numeric_columns:

            selected_category = st.selectbox(
                "Category field",
                list(category_df.columns),
            )

            selected_metric = st.selectbox(
                "Metric",
                numeric_columns,
            )

            fig = px.bar(
                category_df,
                x=selected_category,
                y=selected_metric,
                title="Category comparison",
            )

            st.plotly_chart(
                style_chart(fig),
                use_container_width=True,
            )

        st.dataframe(
            category_df,
            use_container_width=True,
            hide_index=True,
        )

    else:
        st.info("No category metrics are available yet.")

    st.markdown("### Cart metrics")

    if not cart_metrics_df.empty:
        st.dataframe(
            cart_metrics_df,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No cart metrics are available yet.")

    st.markdown("### Cart value distribution")

    if not cart_distribution_df.empty:

        numeric_columns = list(
            cart_distribution_df.select_dtypes(
                include="number"
            ).columns
        )

        if numeric_columns:

            selected_column = st.selectbox(
                "Choose a numeric field",
                numeric_columns,
                key="distribution_column",
            )

            fig = px.histogram(
                cart_distribution_df,
                x=selected_column,
                nbins=25,
                title="Distribution of selected values",
            )

            st.plotly_chart(
                style_chart(fig),
                use_container_width=True,
            )

        st.dataframe(
            cart_distribution_df,
            use_container_width=True,
            hide_index=True,
        )

    else:
        st.info(
            "Cart value distribution is not available yet."
        )

    st.caption(
        "The source API does not establish a currency for every "
        "calculated cart value. Values are displayed without "
        "assuming a specific currency."
    )


# =========================================================
# DATA QUALITY
# =========================================================

elif page == "Data Quality":

    st.title("Data Quality Center")

    st.caption(
        "Review validation results, investigate alerts "
        "and download reports."
    )

    if not quality_report:

        st.warning(
            "No quality report found. Run the pipeline first."
        )

    else:

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Overall status", quality_status)

        with col2:
            st.metric("Recognized alerts", len(alerts))

        st.markdown("### Alert review")

        if alerts:

            alert_frame = pd.DataFrame(
                [
                    {
                        "Alert": alert_to_text(item),
                        "Details": json.dumps(
                            item,
                            default=str,
                        ) if isinstance(item, dict) else str(item),
                    }
                    for item in alerts
                ]
            )

            search_text = st.text_input(
                "Search alerts",
                placeholder="Search by keyword...",
            )

            if search_text:
                mask = alert_frame["Alert"].str.contains(
                    search_text,
                    case=False,
                    na=False,
                )

                alert_frame = alert_frame[mask]

            st.dataframe(
                alert_frame[["Alert"]],
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "Download alert entries",
                data=alert_frame.to_csv(index=False),
                file_name="datasentinel_alerts.csv",
                mime="text/csv",
            )

        else:
            st.info(
                "No standard alert list was found in the report. "
                "Inspect the complete report below."
            )

        st.markdown("### Report downloads")

        col1, col2, col3 = st.columns(3)

        with col1:
            download_json(
                quality_report,
                "data_quality_report.json",
            )

        with col2:
            anomaly_path = latest_csv(
                "cart_anomalies_*.csv"
            )

            if anomaly_path:
                st.download_button(
                    "Download cart anomalies",
                    data=anomaly_path.read_bytes(),
                    file_name=anomaly_path.name,
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.caption("No anomaly CSV found.")

        with col3:
            stock_path = latest_csv(
                "low_stock_products_*.csv"
            )

            if stock_path:
                st.download_button(
                    "Download low-stock report",
                    data=stock_path.read_bytes(),
                    file_name=stock_path.name,
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.caption("No low-stock CSV found.")

        with st.expander("View complete quality report"):
            st.json(quality_report)

    st.markdown("### Low-stock products")

    if not low_stock_df.empty:
        st.dataframe(
            low_stock_df,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No low-stock dataset is available.")


# =========================================================
# AI INVESTIGATION
# =========================================================

elif page == "AI Investigation":

    st.title("AI Investigation Engine")

    st.caption(
        "Use Gemini to investigate the latest data-quality report."
    )

    st.markdown(
        """
        <div class="hero">
            <div class="eyebrow">
                AI-ASSISTED ROOT CAUSE ANALYSIS
            </div>
            <div class="hero-title" style="font-size:26px;">
                Turn alerts into actionable insights
            </div>
            <div class="hero-subtitle">
                Generate an executive summary, explore potential causes,
                recommend corrective actions and identify preventive
                controls using your configured Gemini integration.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if quality_report_path:
        st.success(
            f"Latest report: {quality_report_path.name}"
        )
    else:
        st.warning(
            "No quality report found. Run the pipeline first."
        )

    if st.button(
        "Run AI Investigation",
        type="primary",
        disabled=not bool(quality_report),
        use_container_width=True,
    ):

        try:

            with st.spinner(
                "Gemini is investigating your quality report..."
            ):

                report_data = get_latest_quality_report()

                if not report_data:
                    report_data = quality_report

                findings = investigate_data_quality(
                    report_data
                )

                st.session_state["ai_findings"] = findings

        except Exception as error:
            st.error(
                f"AI investigation failed: {error}"
            )

    findings = st.session_state.get("ai_findings")

    if findings:

        st.markdown("### Investigation findings")

        if isinstance(findings, dict):
            st.json(findings)
        elif isinstance(findings, list):
            st.json(findings)
        else:
            st.markdown(str(findings))

        download_json(
            findings,
            "datasentinel_ai_investigation.json",
        )

        st.caption(
            "AI-generated root causes are hypotheses until "
            "verified against source data, pipeline logs and "
            "business rules."
        )

    else:
        st.info(
            "Click Run AI Investigation to analyze the latest report. "
            "The request uses your configured Gemini API and may "
            "consume API quota."
        )


# =========================================================
# DATA EXPLORER
# =========================================================

elif page == "Data Explorer":

    st.title("Data Explorer")

    st.caption(
        "Search, inspect and download available datasets."
    )

    products_tab, carts_tab, gold_tab, report_tab = st.tabs(
        [
            "Products",
            "Carts",
            "Gold datasets",
            "Quality report",
        ]
    )

    with products_tab:

        st.markdown("### Bronze products")

        if not products_df.empty:

            search = st.text_input(
                "Search products",
                placeholder="Search by product name or field...",
                key="products_search",
            )

            filtered_df = products_df.copy()

            if search:

                text_columns = list(
                    filtered_df.select_dtypes(
                        include=["object", "string"]
                    ).columns
                )

                if text_columns:

                    mask = pd.Series(
                        False,
                        index=filtered_df.index,
                    )

                    for column in text_columns:
                        mask |= filtered_df[column].astype(
                            str
                        ).str.contains(
                            search,
                            case=False,
                            na=False,
                        )

                    filtered_df = filtered_df[mask]

            st.dataframe(
                filtered_df,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "Download products CSV",
                data=filtered_df.to_csv(index=False),
                file_name="products.csv",
                mime="text/csv",
            )

        else:
            st.info("No product data found.")

    with carts_tab:

        st.markdown("### Bronze carts")

        if not carts_df.empty:

            st.dataframe(
                carts_df,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "Download carts CSV",
                data=carts_df.to_csv(index=False),
                file_name="carts.csv",
                mime="text/csv",
            )

        else:
            st.info("No cart data found.")

    with gold_tab:

        st.markdown("### Gold datasets")

        datasets = {
            "Product category metrics": category_df,
            "Low-stock products": low_stock_df,
            "Cart metrics": cart_metrics_df,
            "Cart value distribution": cart_distribution_df,
        }

        selected_dataset = st.selectbox(
            "Select dataset",
            list(datasets.keys()),
        )

        selected_df = datasets[selected_dataset]

        if not selected_df.empty:

            st.dataframe(
                selected_df,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "Download selected dataset",
                data=selected_df.to_csv(index=False),
                file_name=(
                    selected_dataset.lower()
                    .replace(" ", "_")
                    + ".csv"
                ),
                mime="text/csv",
            )

        else:
            st.info("This dataset is not available yet.")

    with report_tab:

        st.markdown("### Complete quality report")

        if quality_report:

            st.json(quality_report)

            download_json(
                quality_report,
                "data_quality_report.json",
            )

        else:
            st.info("No quality report found.")


# =========================================================
# FOOTER
# =========================================================

st.markdown(
    """
    <div class="footer">
        DATASENTINEL AI
        &nbsp;·&nbsp;
        LIVE DATA INGESTION
        &nbsp;·&nbsp;
        PYSPARK
        &nbsp;·&nbsp;
        DATA QUALITY
        &nbsp;·&nbsp;
        GEMINI AI
    </div>
    """,
    unsafe_allow_html=True,
)