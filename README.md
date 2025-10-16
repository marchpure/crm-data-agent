# CRM Data Q&A Agent - Advanced RAG with NL2SQL over Salesforce Data

| | |
| ------------ | ------------- |
| <img src="src/web/images/logo-dark.svg" width="256"/> | This is a 📊 Data Analytics Agent that grounds its conversation in Salesforce data replicated to a Data Warehouse in BigQuery.    |

The agent demonstrates an advanced [Retrieval-Augmented Generation](https://cloud.google.com/use-cases/retrieval-augmented-generation) workflow
in a multi-agentic system with contextualized Natural-Language-to-SQL
components powered by Long Context and In-Context Learning capabilities of [Gemini 2.5 Pro](https://deepmind.google/technologies/gemini).

🚀 **Blog post**: [Forget vibe coding, vibe Business Intelligence is here!](https://medium.com/@vladkol_eqwu/business-intelligence-in-ai-era-how-agents-and-gemini-unlock-your-data-ce158081c678)

The agent is built with [Agent Development Kit](https://google.github.io/adk-docs/).

* The agent interprets questions about state of the business how it's reflected in CRM rather than directly referring to Salesforce data entities.
* It generates SQL query to gather data necessary for answering the question
* It creates interactive [Vega-Lite](https://vega.github.io/vega-lite/) diagrams.
* It analyzes the results, provides key insights and recommended actions.

<a href="tutorial/img/screenshot-dark.png">
<img src="tutorial/img/screenshot-dark.png" alt="What are our best lead source in every country?" style="width:900px;"/>
</a>

## Agent Development Kit

<img src="https://google.github.io/adk-docs/assets/agent-development-kit.png" style="width:64px;"/>

The agent is built using [Agent Development Kit](https://google.github.io/adk-docs/) (ADK) - a flexible
and modular framework for developing and deploying AI agents.

The sample also demonstrates:

* How to build a Web UI for ADK-based data agents using [streamlit](https://streamlit.io/).
* How to use [Artifact Services](https://google.github.io/adk-docs/artifacts/) with ADK.
* How to stream and interpret session [events](https://google.github.io/adk-docs/events/).
* How to create and use a custom [Session Service](https://google.github.io/adk-docs/sessions/session/).

## 🕵🏻‍♀️ Simple questions are complex

<img src="tutorial/img/top_5_customers.jpg" alt="Top 5 customers by impact in the US this year" style="width:800px;"/>

### Examples of questions the agent can answer

* "Top 5 customers in every country"
* "What are our best lead sources?"
  * or more specific "What are our best lead sources by value?"
* Lead conversion trends in the US.

### High-Level Design

<img src="tutorial/img/data_agent_design.jpg" alt="Top 5 customers in every country" style="width:800px;"/>

## 🚀 Deploy and Run

To deploy the sample with demo data to a publicly available Cloud Run service,
use `Run on Google Cloud` button below.

[![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://console.cloud.google.com/cloudshell/?cloudshell_git_repo=https://github.com/vladkol/crm-data-agent&cloudshell_image=gcr.io/cloudrun/button&show=terminal&utm_campaign=CDR_0xc245fc42_default_b417442301&utm_medium=external&utm_source=blog)

You need a Google Cloud Project with a [Billing Account](https://console.cloud.google.com/billing?utm_campaign=CDR_0xc245fc42_default_b417442301&utm_medium=external&utm_source=blog).

### Manual deployment

* Clone this repository:

```bash
git clone https://github.com/vladkol/crm-data-agent && cd crm-data-agent
```

* Create a Python virtual Environment

> [uv](https://docs.astral.sh/uv/) makes it easy: `uv venv .venv --python 3.11 && source .venv/bin/activate`

* Install dependencies

`pip install -r src/requirements.txt`

or, with `uv`:

`uv pip install -r src/requirements.txt`

* Create `.env` file in `src` directory. Set configuration values as described below.

> [src/.env-template](src/.env-template) is a template to use for your `.env` file.

### Environment Variables

Before running the application, you need to set up the necessary environment variables. Copy the `src/.env-template` file to `src/.env` and fill in the required values:

- `ARK_API_KEY`: [REQUIRED] Your Volcengine Ark API Key.
- `VE_LLM_MODEL_ID`: [REQUIRED] The model endpoint ID from the Volcengine Ark platform (e.g., `doubao-pro-32k`).
- `EMR_CATALOG`: [REQUIRED] The catalog name in your EMR Serverless Presto instance.
- `EMR_DATABASE`: [REQUIRED] The database name within the specified catalog that contains your CRM data.

Optional:
- `ARK_BASE_URL`: The base URL for the Volcengine Ark API. Defaults to `https://ark.cn-beijing.volces.com/api/v3`.

**If you deploy the agent to Cloud Run**, its service account must have the following roles:

* BigQuery Job User (`roles/bigquery.jobUser`) in BQ_PROJECT_ID project (or GOOGLE_CLOUD_PROJECT, if BQ_PROJECT_ID is not defined).
* BigQuery Data Viewer (`roles/bigquery.dataViewer`) for SFDC_BQ_DATASET dataset.
* Storage Object User (`roles/storage.objectUser`) for AI_STORAGE_BUCKET bucket.
* Vertex AI User (`roles/aiplatform.user`) in GOOGLE_CLOUD_PROJECT project.

### Enable APIs in your project

```bash
gcloud services enable \
    aiplatform.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    firestore.googleapis.com \
    bigquery.googleapis.com \

    --project=[GOOGLE_CLOUD_PROJECT]
```

> Replace `[GOOGLE_CLOUD_PROJECT]` with GOOGLE_CLOUD_PROJECT value you put in `src/.env` file.

### Deploy Salesforce Data

#### Demo data

Run `utils/deploy_demo_data.py` script.

> **Note**: Demo data contains records dated 2020-2022. If you ask questions with "last year" or "6 months ago", they will likely return no data.

#### Real Salesforce Data

Create a [BigQuery Data Transfer for Salesforce](https://cloud.google.com/bigquery/docs/salesforce-transfer).

Make sure you transfer the following objects:

* Account
* Case
* CaseHistory
* Contact
* CurrencyType
* DatedConversionRate
* Event
* Lead
* Opportunity
* OpportunityHistory
* RecordType
* Task
* User

#### Deployment with your custom Salesforce.com metadata

*COMING SOON!*

This will allow you to use your customized metadata in addition to analyzing your real data replicated to BigQuery.

### Run Locally

* Run `.\run_local.sh`
* Open `http://localhost:8080` in your browser.

#### Deploy and Run in Cloud Run

* Run `.\deploy_to_cloud_run.sh`

> This deployment uses default Compute Service Account for Cloud Run.
To make changes in how the deployment is done, adjust `gcloud` command in [deploy_to_cloud_run.py](utils/deploy_to_cloud_run.py)

**Cloud Run Authentication Note**:

By default, this script deploys a Cloud Run service that requires authentication.
You can switch to unauthenticated mode in [Cloud Run](https://console.cloud.google.com/run) or configure a [Load Balancer and Identity Access Proxy](https://cloud.google.com/iap/docs/enabling-cloud-run) (recommended).

## 📃 License

This repository is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## 🗒️ Disclaimers

This is not an officially supported Google product. This project is not eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

Code and data from this repository are intended for demonstration purposes only. It is not intended for use in a production environment.
