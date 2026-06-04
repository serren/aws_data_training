from io import BytesIO
import pandas as pd
import boto3
import io

bucket_name = "my-test-bucket"
input_prefix = "csv-data/"
output_prefix = "parquet-data/"

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)

if 'Contents' not in response:
    print("No files found in", input_prefix)
else:
    for obj in response['Contents']:
        key = obj['Key']
        
        if not key.lower().endswith('.csv'):
            continue
        
        # csv-data/order-service.csv → order-service
        file_name = key.split('/')[-1]
        name_without_ext = file_name.rsplit('.', 1)[0]
        
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))
        
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        output_key = f"{output_prefix}{name_without_ext}/data.parquet"
        s3_resource.Object(bucket_name, output_key).put(Body=parquet_buffer.getvalue())
        
        print(f"Converted: {key} → s3://{bucket_name}/{output_key} ({len(df)} rows)")

print("Done!")