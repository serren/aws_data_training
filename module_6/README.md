# Module 6 — Kinesis Output Stream + Firehose to S3

Extends module 5: the Flink application now writes windowed metrics to a second Kinesis stream instead of logging them. A Firehose delivery stream reads from that output stream and stores JSON records in S3 with dynamic partitioning by `componentName`.

## Architecture

```
Kinesis input stream (smeranovich-kinesis-stream)
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
Kinesis output stream (smeranovich-kinesis-output-stream)
        │
        ▼
Kinesis Data Firehose (smeranovich-metrics-firehose)
        │  dynamic partitioning by componentName
        ▼
S3 bucket (smeranovich-kinesis-output)
  └── {componentName}/
          └── <firehose-files>.json
```

## Prerequisites

- AWS CLI configured with sufficient permissions
- S3 bucket `smeranovich-kinesis-app` already exists (deploy `kinesis-bucket.yaml` from module 5 if it doesn't)
- Maven 3.x, Java 11

## Deploy

### 1. Build the JAR

```bash
cd module_6
mvn package -DskipTests
```

### 2. Upload JAR to S3

```bash
aws s3 cp target/flink-metrics-module6-1.0-SNAPSHOT.jar \
  s3://smeranovich-kinesis-app/flink-app-6/flink-metrics-module6-1.0-SNAPSHOT.jar
```

### 3. Deploy the CloudFormation stack

> If the module 5 stack is still running, delete it first — both stacks use the same stream name `smeranovich-kinesis-stream` and KDA application name `smeranovich-metrics-processor`.

```bash
aws cloudformation deploy \
  --stack-name smeranovich-kinesis-stack \
  --template-file kinesis-analytics.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
      KinesisStreamRegion=us-east-1 \
      ApplicationJarKey=flink-app-6/flink-metrics-module6-1.0-SNAPSHOT.jar
```

Wait for the stack to reach `CREATE_COMPLETE`:

```bash
aws cloudformation wait stack-create-complete \
  --stack-name smeranovich-kinesis-stack
```

### 4. Start the Flink application

```bash
aws kinesisanalyticsv2 start-application \
  --application-name smeranovich-metrics-processor \
  --run-configuration '{}'
```

Wait until the application status is `RUNNING`:

```bash
aws kinesisanalyticsv2 describe-application \
  --application-name smeranovich-metrics-processor \
  --query 'ApplicationDetail.ApplicationStatus'
```

## Verify

### Check CloudWatch logs

```bash
aws logs tail /aws/kinesis-analytics/smeranovich-metrics-processor --follow
```

### Send test data to the input stream

Use the generator in `test_data_generator/` or put a record manually:

```bash
aws kinesis put-record \
  --stream-name smeranovich-kinesis-stream \
  --partition-key 1 \
  --data "$(echo '{"componentName":"user-service","metricName":"cpu","value":42.0,"unit":"percent","publicationTimestamp":"2024-01-01T12:00:00Z"}' | base64)"
```

### Check the output Kinesis stream

```bash
SHARD_ITERATOR=$(aws kinesis get-shard-iterator \
  --stream-name smeranovich-kinesis-output-stream \
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
aws s3 ls s3://smeranovich-kinesis-output/ --recursive
```

Files are partitioned by component name:

```
user-service/2024/...
order-service/2024/...
```

## Stop

```bash
aws kinesisanalyticsv2 stop-application \
  --application-name smeranovich-metrics-processor \
  --force
```

## Teardown

```bash
aws cloudformation delete-stack --stack-name smeranovich-kinesis-stack
aws cloudformation wait stack-delete-complete --stack-name smeranovich-kinesis-stack
```

> The S3 output bucket (`smeranovich-kinesis-output`) must be empty before the stack can be deleted. Empty it first if needed:
> ```bash
> aws s3 rm s3://smeranovich-kinesis-output --recursive
> ```

## Running tests locally

Requires Docker (WSL2). See `DOCKER.md` in the project root for setup.

```bash
export DOCKER_HOST="tcp://$(wsl hostname -I | awk '{print $1}' | tr -d '\r'):2375"
mvn test
```
