# Module 6 — Kinesis Output Stream + Firehose to S3

Extends module 5: the Flink application now writes windowed metrics to a second Kinesis stream instead of logging them. A Firehose delivery stream reads from that output stream and stores JSON records in S3 with dynamic partitioning by `componentName`.

## Architecture

```
Kinesis input stream (my-test-kinesis-stream)
        │
        ▼
  Flink app (KDA)
  ┌─────────────────────────────────────────────────┐
  │  FlinkKinesisConsumer                           │
  │       │                                         │
  │  TumblingEventTimeWindows (5 min)               │
  │       │                                         │
  │  KinesisSinkFunction  ──►  output stream        │
  └─────────────────────────────────────────────────┘
        │
        ▼
Kinesis output stream (my-test-kinesis-output-stream)
        │
        ▼
Kinesis Data Firehose (my-test-metrics-firehose)
        │  dynamic partitioning by componentName
        ▼
S3 bucket (my-test-kinesis-output)
  └── {componentName}/
          └── <firehose-files>.json
```

## Prerequisites

- AWS CLI configured with sufficient permissions
- S3 bucket `my-test-kinesis-app` already exists (deploy `kinesis-bucket.yaml` from module 5 if it doesn't)
- Maven 3.x, Java 11

## Deploy

### 1. Build the JAR

```bash
cd module_6
mvn package -DskipTests
```

### 2. Upload JAR to S3

Create S3 bucket:
```bash
aws cloudformation deploy  --template-file kinesis-bucket.yaml \
  --stack-name my-test-kinesis-bucket \
  --region us-east-1
```
Deploy JAR:
```bash
aws s3 cp target/flink-metrics-module6-1.0-SNAPSHOT.jar \
  s3://my-test-kinesis-app/flink-app-6/flink-metrics-module6-1.0-SNAPSHOT.jar
```

### 3. Deploy the CloudFormation stack

```bash
aws cloudformation deploy \
  --stack-name my-test-kinesis-stack \
  --template-file kinesis-analytics.yaml \
  --capabilities CAPABILITY_IAM \
  --region us-east-1
```

Wait for the stack to reach `CREATE_COMPLETE`:

```bash
aws cloudformation wait stack-create-complete \
  --stack-name my-test-kinesis-stack
```

### 4. Start the Flink application

```bash
aws kinesisanalyticsv2 start-application \
  --application-name my-test-metrics-processor \
  --run-configuration '{}'
```

Wait until the application status is `RUNNING`:

```bash
aws kinesisanalyticsv2 describe-application \
  --application-name my-test-metrics-processor \
  --query 'ApplicationDetail.ApplicationStatus'
```

## Verify

### Check CloudWatch logs

```bash
aws logs tail /aws/kinesis-analytics/my-test-metrics-processor --follow
```

### Send test data to the input stream

Use the generator in `test_data_generator/` or put a record manually:

```bash
aws kinesis put-record \
  --stream-name my-test-kinesis-stream \
  --partition-key 1 \
  --data "$(echo '{"componentName":"user-service","metricName":"cpu","value":42.0,"unit":"percent","publicationTimestamp":"2024-01-01T12:00:00Z"}' | base64)"
```

### Check the output Kinesis stream

```bash
SHARD_ITERATOR=$(aws kinesis get-shard-iterator \
  --stream-name my-test-kinesis-output-stream \
  --shard-id shardId-000000000000 \
  --shard-iterator-type TRIM_HORIZON \
  --query ShardIterator --output text)

aws kinesis get-records \
  --shard-iterator "$SHARD_ITERATOR" \
  --query 'Records[].Data' --output text \
  | base64 --decode
```

### Check S3 output

Records appear in S3 after the Firehose buffering interval (60 seconds by default):

```bash
aws s3 ls s3://my-test-kinesis-output/ --recursive
```

Files are partitioned by component name:

```
user-service/2024/...
order-service/2024/...
```

## Stop

```bash
aws kinesisanalyticsv2 stop-application \
  --application-name my-test-metrics-processor \
  --force
```

## Teardown

```bash
aws cloudformation delete-stack --stack-name my-test-kinesis-stack
aws cloudformation wait stack-delete-complete --stack-name my-test-kinesis-stack
```

> The S3 output bucket (`my-test-kinesis-output`) must be empty before the stack can be deleted. Empty it first if needed:
> ```bash
> aws s3 rm s3://my-test-kinesis-output --recursive
> ```

## Running tests locally

Requires Docker (WSL2). See `DOCKER.md` in the project root for setup.

```bash
export DOCKER_HOST="tcp://$(wsl hostname -I | awk '{print $1}' | tr -d '\r'):2375"
mvn test
```
