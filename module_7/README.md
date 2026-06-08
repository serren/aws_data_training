# Module 7 — Server Access Log Analytics (EMR Spark Job)

Spark batch job that reads server access logs from S3, aggregates call counts per
service pair, and writes traffic reports back to S3 in NDJSON format.

## What it does

1. Reads all `*.txt` log files from an S3 input bucket (`{service}-YYYY-MM-DD.txt`)
2. Extracts the **target** service name from the file name and the **source** service
   name from each log line
3. Filters out user-originated traffic (only service-to-service calls are counted)
4. Filters out records older than a configurable time window (default: last 24 hours)
5. Counts requests per `(source, target)` pair
6. Writes one JSON record per pair to the S3 output bucket

### Input log format

```
<masked>  - order-service "POST /api/v1/audit" 200 [2022-09-15T13:43:01.721Z] "Apache HTTP Client"
```

File name encodes the **target** service: `audit-service-2022-09-15.txt`

### Output record format

```json
{"id":"a3f1c2d4e5b6f7a8","source":"order-service","target":"audit-service","totalRequests":42}
```

`id` — first 16 characters of `MD5(source + target)`, stable across runs.

## Project structure

```
module_7/
  stack.yaml                         # CloudFormation stack (all AWS resources)
  job-driver.json                    # EMR Serverless job submission payload
  grafana/
    docker-compose.yml               # Grafana container (port 3001)
    .env.example                     # AWS credentials template
    provisioning/
      datasources/athena.yaml        # Athena datasource → my_test_emr_db
      dashboards/dashboard.yaml      # dashboard provider config
      dashboards/traffic.json        # Service Call Graph (Node Graph panel)
  src/
    main/java/com/example/spark/
      SparkJob.java                  # entry point + DataFrame pipeline
    test/java/com/example/spark/
      SparkJobTest.java              # integration test (LocalStack S3 via Testcontainers)
      TrafficReport.java             # POJO for deserializing test output
    test/resources/
      log4j2-test.xml                # suppresses Spark/Hadoop noise in test output
```

## Run tests

Requires Docker (WSL2 on Windows — see below).

```bash
DOCKER_HOST=tcp://$(wsl hostname -I | awk '{print $1}'):2375 mvn test
```

The integration test spins up a LocalStack S3 container, uploads synthetic log
files, runs the Spark job in `local[*]` mode, and asserts the output records.

### Windows / WSL2 Docker note

Maven uses the JDK configured in your IDE (tested with Corretto 25). Docker must
be running inside WSL2 and reachable via TCP. Set `DOCKER_HOST` as shown above
before running `mvn test`.

## Build

```bash
mvn package -DskipTests
```

Produces `target/module_7-1.0-SNAPSHOT.jar` — a fat JAR with `hadoop-aws` and
`aws-java-sdk-bundle` bundled in. Spark itself is excluded (`provided` scope)
because EMR supplies it.

## Deploy to AWS

All AWS resources are defined in `stack.yaml` and created with a single command.

### 1. Deploy the CloudFormation stack

```bash
aws cloudformation deploy \
  --template-file stack.yaml \
  --stack-name my-test-module7 \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1 \
  --parameter-overrides EMRReleaseLabel=emr-7.4.0
```

Resources created:

| Resource | Name |
|---|---|
| S3 — source code | `my-test-source-code-{AccountId}` |
| S3 — access logs input | `my-test-access-logs-{AccountId}` |
| S3 — traffic reports output | `my-test-traffic-reports-{AccountId}` |
| S3 — Athena query results | `my-test-athena-output-{AccountId}` |
| IAM role | `my-test-emr-execution-role` |
| Glue database | `my_test_emr_db` |
| Glue table | `my_test_traffic_reports` |
| EMR Serverless application | `my-test-spark-app` |

> **EMR Studio** — not in the template (requires VPC/subnet). Create manually in the
> console: EMR → Studios → Create Studio. Takes ~2 minutes and is free to keep.

**Capacity pitfalls** (already handled in `stack.yaml`):
- Do **not** set `InitialCapacity` — CloudFormation rejects an empty map; omit the
  property entirely to disable pre-initialized capacity
- `MaximumCapacity` units must include a space: `3 vCPU`, `4 GB`, `60 GB`

### 2. Upload test data

Generate logs with the test data generator, then sync to S3:

```bash
# from test_data_generator/
# Before running, update "startTime" in server-access-log.json to a recent timestamp,
# e.g. "2026-06-06T13:00:00.000Z" — the job filters the last 24 hours by default,
# so logs with old timestamps will be silently excluded.
java -jar test-data-generator-1.0.0-all.jar server-access-log.json

aws s3 sync test-output/ s3://my-test-access-logs-{AccountId}/ --region us-east-1
```

### 3. Upload the JAR

```bash
aws s3 cp target/module_7-1.0-SNAPSHOT.jar \
  s3://my-test-source-code-{AccountId}/jars/ --region us-east-1
```

### 4. Submit the job

On **Linux / macOS**:

```bash
aws emr-serverless start-job-run \
  --application-id <app-id> \
  --execution-role-arn <role-arn> \
  --region us-east-1 \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://my-test-source-code-{AccountId}/jars/module_7-1.0-SNAPSHOT.jar",
      "entryPointArguments": [
        "s3://my-test-access-logs-{AccountId}",
        "s3://my-test-traffic-reports-{AccountId}",
        "24"
      ],
      "sparkSubmitParameters": "--class com.example.spark.SparkJob --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.driver.cores=1 --conf spark.driver.memory=1g --conf spark.executor.instances=1"
    }
  }'
```

On **Windows (PowerShell)** — use a file to avoid JSON parsing issues:

```powershell
@"
{
  "sparkSubmit": {
    "entryPoint": "s3://my-test-source-code-{AccountId}/jars/module_7-1.0-SNAPSHOT.jar",
    "entryPointArguments": [
      "s3://my-test-access-logs-{AccountId}",
      "s3://my-test-traffic-reports-{AccountId}",
      "24"
    ],
    "sparkSubmitParameters": "--class com.example.spark.SparkJob --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.driver.cores=1 --conf spark.driver.memory=1g --conf spark.executor.instances=1"
  }
}
"@ | Out-File -FilePath job-driver.json -Encoding utf8

aws emr-serverless start-job-run `
  --application-id <app-id> `
  --execution-role-arn <role-arn> `
  --region us-east-1 `
  --job-driver file://job-driver.json
```

| Argument | Description |
|---|---|
| `inputPath` | S3 path to the folder containing `*.txt` log files |
| `outputPath` | S3 path where `reports/` will be written |
| `hoursBack` | Time window in hours (optional, default `24`) |

Get `<app-id>` and `<role-arn>` from stack outputs:

```bash
aws cloudformation describe-stacks --stack-name my-test-module7 \
  --query 'Stacks[0].Outputs' --region us-east-1
```

The first run takes 2–3 minutes while EMR provisions cold capacity.

### Output location

```
s3://my-test-traffic-reports-{AccountId}/reports/part-00000-*.json
                                                  _SUCCESS
```

## Visualise with Grafana

A self-contained Grafana setup lives in `grafana/` (port **3001**).
Prereqs: CloudFormation stack deployed and EMR job run at least once.

```bash
cd grafana
cp .env.example .env
# Fill in AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
# AWS_DEFAULT_REGION (default: us-east-1), and ACCOUNT_ID (your 12-digit AWS account ID).
docker compose up -d
```

Open http://localhost:3001 — login `admin` / `admin`.

The **Service Call Graph** dashboard is provisioned automatically. It shows a Node Graph
panel with one circle per service and one directed edge per `(source → target)` pair,
labelled with `totalRequests`.

## Teardown

Empty the buckets first (CloudFormation cannot delete non-empty S3 buckets), then
delete the stack:

```bash
for bucket in \
  my-test-access-logs-{AccountId} \
  my-test-traffic-reports-{AccountId} \
  my-test-source-code-{AccountId} \
  my-test-athena-output-{AccountId}; do
  aws s3 rm s3://$bucket --recursive --region us-east-1
done

aws cloudformation delete-stack --stack-name my-test-module7 --region us-east-1
```

## Key dependencies

| Artifact | Version | Scope |
|---|---|---|
| `spark-core_2.12` / `spark-sql_2.12` | 3.5.3 | provided |
| `hadoop-aws` | 3.3.6 | compile (bundled) |
| `aws-java-sdk-bundle` | 1.12.400 | compile (bundled) |
| `testcontainers:localstack` | 1.20.6 | test |
