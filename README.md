# DataSentinel AI 🚀

### AI-Powered Data Quality Monitoring & Pipeline Investigation

DataSentinel AI is a data engineering and AI project that ingests live product and cart data, transforms it through a Bronze–Silver–Gold architecture, monitors data quality, and uses Google Gemini to investigate quality alerts and recommend actionable checks.

🔗 **Live Dashboard:** [Launch DataSentinel AI](https://datasentinel-ai-kudzzpeb7j8sjyrdaedzk8.streamlit.app/)

📂 **GitHub Repository:** [payalpandey565/datasentinel-ai](https://github.com/payalpandey565/datasentinel-ai)

---

## 📌 Project Overview

Data pipelines can produce unexpected values, inventory anomalies, and data-quality issues that require investigation.

DataSentinel AI combines automated data processing, quality checks, business analytics, and generative AI to help users explore anomalies and understand what to investigate next.

The application provides a dashboard for monitoring pipeline execution, reviewing quality reports, exploring processed data, and generating AI-assisted investigation findings.

## ✨ Key Features

### 1. Live Data Ingestion

* Retrieves product and cart data from a live API.
* Supports paginated API ingestion.
* Organizes ingested records into timestamped Bronze-layer runs.

### 2. Medallion Data Architecture

* **Bronze:** Stores ingested source data.
* **Silver:** Applies data transformations using PySpark.
* **Gold:** Produces analytics-ready datasets and business metrics.

### 3. Data Quality Monitoring

* Generates data-quality reports.
* Identifies unusually high cart values for review.
* Flags products with low stock.
* Provides evidence to support further investigation.

### 4. AI-Powered Investigation

* Integrates Google Gemini through the Google Gen AI SDK.
* Analyzes generated data-quality reports.
* Produces potential root-cause hypotheses.
* Recommends investigation steps and remediation actions.
* Highlights limitations and uncertainty rather than treating every anomaly as a confirmed error.

### 5. Interactive Dashboard

The Streamlit dashboard includes:

* Overview
* Pipeline Monitor
* Business Analytics
* Data Quality
* AI Investigation
* Data Explorer

### 6. Business Analytics

* Product-category metrics
* Inventory-health insights
* Cart-value distribution
* Summary metrics for downstream analysis

---

## 🏗️ Architecture

```text
                  Live Product & Cart API
                            |
                            v
                  +-------------------+
                  | Bronze Layer      |
                  | Raw API Data      |
                  +-------------------+
                            |
                            v
                  +-------------------+
                  | Silver Layer      |
                  | PySpark Processing|
                  +-------------------+
                            |
                            v
                  +-------------------+
                  | Gold Layer        |
                  | Analytics Tables  |
                  +-------------------+
                            |
                 +----------+-----------+
                 |                      |
                 v                      v
       +------------------+   +--------------------+
       | Data Quality     |   | Business Analytics |
       | Reports & Alerts |   | Metrics & Insights |
       +------------------+   +--------------------+
                 |
                 v
       +----------------------+
       | Google Gemini        |
       | AI Investigation     |
       +----------------------+
                 |
                 v
       +----------------------+
       | Streamlit Dashboard  |
       +----------------------+
```

## 🛠️ Technology Stack

| Technology                      | Purpose                                 |
| ------------------------------- | --------------------------------------- |
| Python                          | Application and pipeline logic          |
| SQL / Data Engineering Concepts | Data processing and validation concepts |
| PySpark                         | Silver-layer transformations            |
| Pandas                          | Data handling and output preparation    |
| PyArrow                         | Parquet data storage                    |
| Streamlit                       | Interactive dashboard                   |
| Plotly                          | Data visualizations                     |
| Google Gemini                   | AI-assisted data-quality investigation  |
| Google Gen AI SDK               | Gemini API integration                  |
| REST APIs                       | Live data ingestion                     |
| Git & GitHub                    | Version control and project hosting     |
| Streamlit Community Cloud       | Dashboard deployment                    |

## 📊 Data Quality Checks

The project includes checks for issues such as:

* Unusually high cart values
* Products with low stock
* Potential anomalies that require investigation

These checks identify records for review; an alert does not necessarily mean the underlying record is incorrect.

## 🤖 AI Investigation Workflow

1. Execute the data pipeline.
2. Generate a data-quality report.
3. Load the latest available report.
4. Send the report to Google Gemini.
5. Generate an investigation containing:

   * Executive summary
   * Alert investigation
   * Root-cause hypotheses
   * Recommended actions
   * Pipeline prevention measures
   * Investigation limitations

The AI output is intended to support engineering investigation, not replace validation or establish root causes without evidence.

## 📸 Application Screenshots

Screenshots are available in the [`screenshots/`](screenshots/) directory.

| Dashboard Area     | Screenshot                              |
| ------------------ | --------------------------------------- |
| Overview           | `screenshots/01_dashboard_overview.png` |
| Pipeline Monitor   | `screenshots/02_pipeline_monitor.png`   |
| Business Analytics | `screenshots/03_business_analytics.png` |
| Data Quality       | `screenshots/04_data_quality.png`       |
| AI Investigation   | `screenshots/05_ai_investigation.png`   |
| Data Explorer      | `screenshots/06_data_explorer.png`      |

*Ensure the filenames match the actual files in the repository. If your screenshots use different names, update this table accordingly.*

## ⚙️ Local Setup

### Prerequisites

* Python version compatible with the installed PySpark version
* Java runtime compatible with PySpark
* Git
* A Google Gemini API key for AI investigation

### 1. Clone the repository

```bash
git clone https://github.com/payalpandey565/datasentinel-ai.git
cd datasentinel-ai
```

### 2. Create a virtual environment

**Windows PowerShell:**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure the Gemini API key

Create a `.env` file in the project root:

```env
GEMINI_API_KEY=your_gemini_api_key
```

Replace the placeholder with your own API key.

**Never commit `.env` or expose your API key publicly.**

### 5. Launch the dashboard

```bash
streamlit run app.py
```

Open the local URL printed by Streamlit in your browser.

## 🔐 Secrets and Configuration

* Local development uses a `.env` file.
* Streamlit Community Cloud should use its Secrets configuration.
* Generated datasets and reports are created during pipeline execution.
* Secret keys and credentials must not be committed to GitHub.

## 🚀 Deployment

The dashboard is deployed using Streamlit Community Cloud.

**Live application:**
https://datasentinel-ai-kudzzpeb7j8sjyrdaedzk8.streamlit.app/

The deployed environment requires compatible Python dependencies, a Java runtime for PySpark, and securely configured Gemini credentials for AI investigation.

Full pipeline execution and AI investigation should be validated in the deployed environment.

## 🎯 Project Goals

This project demonstrates practical experience with:

* API-based data ingestion
* Bronze–Silver–Gold data architecture
* PySpark data transformations
* Parquet-based analytical datasets
* Data-quality monitoring and anomaly detection
* Generative AI integration
* AI-assisted root-cause investigation
* Interactive analytics dashboards
* Cloud application deployment
* Git-based version control

## 🔮 Future Improvements

Potential enhancements include:

* Automated scheduled pipeline execution
* Historical data-quality trend analysis
* Pipeline run history and performance tracking
* More configurable validation rules
* Persistent storage for historical runs
* Enhanced alerting and notifications
* Additional AI-assisted investigation tools

##  Author

**Payal Pandey**

Data Engineering | Python | SQL | PySpark | Azure Databricks | Generative AI

GitHub: [@payalpandey565](https://github.com/payalpandey565)

---

*DataSentinel AI is a portfolio project built to explore the intersection of data engineering, data quality monitoring, analytics, and generative AI.*
