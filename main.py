'''Code to move data from s3 to dynamodb'''

import json
import boto3

s3_client = boto3.client('s3')
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table('Financialdatatest')


def lambda_handler(event, context):
    try:

        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]

        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        data = response["Body"].read().decode('utf-8')
        financialdatatest = data.split("\n")
        for year in financialdatatest:
            year = year.split(",")
            table.put_item(
                Item={
                    "Year": str(year[0]),
                    "Industry_aggregation_NZSIOC": str(year[1]),
                    "Industry_code_NZSIOC": str(year[2]),
                    "Industry_name_NZSIOC": str(year[3]),
                    "Units": str(year[4]),
                    "Variable_code": str(year[5]),
                    "Variable_name": str(year[6]),
                    "Variable_category": str(year[7]),
                    "Value": str(year[8])

                }
            )
    except Exception as err:
        print(err)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
