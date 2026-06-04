# Module 3 — Grafana + Athena dashboard

Visualises windowed metrics from module_6 using Grafana with the Amazon Athena plugin.

## Architecture

```
S3 (my-test-kinesis-output)
        │
        ▼
AWS Glue / Athena
  database: my_test_kinesis_db
  table:    my_test_kinesis_metrics
        │
        ▼
Grafana (Docker, localhost:3000)
  datasource: grafana-athena-datasource
  dashboard:  Kinesis Metrics
```

## Prerequisites

- Docker and Docker Compose
- AWS credentials with access to Athena, Glue, and S3 (`my-test-athena-output`)
- module_6 stack deployed (Glue database + table must exist)

## Setup

### 1. Create `.env` from the example

```bash
cp .env.example .env
```

Fill in your credentials. If you use AWS SSO or a Sandbox account, all three variables are required:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_DEFAULT_REGION=us-east-1
```

### 2. Start Grafana

```bash
docker compose up -d
```

The `grafana-athena-datasource` plugin is installed automatically on first start.

### 3. Open the dashboard

Navigate to http://localhost:3000 (login: `admin` / `admin`).

The **Kinesis Metrics** dashboard is pre-provisioned with:
- **Component** and **Metric** dropdowns to filter data
- **Time-series panel** — avg/min/max values per 5-minute window
- **Raw data table** — last 20 windows, sorted by `totimestamp` descending

## Stopping

```bash
docker compose down
```

To also remove the stored Grafana state (dashboards edited in UI, etc.):

```bash
docker compose down -v
```
