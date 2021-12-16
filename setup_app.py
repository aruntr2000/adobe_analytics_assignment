import zipfile
import boto3
from botocore.exceptions import ClientError
import time
import awswrangler as wr

s3Client = boto3.client('s3')
s3 = boto3.resource('s3')
lambdaClient = boto3.client('lambda')

def create_s3_bucket_if_not_exists(bucket_name):
    try:
        s3Client.head_bucket(Bucket=bucket_name)
        print(f'S3 bucket already exist: {bucket_name}')
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404' or error_code == 'NoSuchBucket':
            print(f'Creating S3 bucket: {bucket_name}')
            s3Client.create_bucket(Bucket=bucket_name)
            time.sleep(15)

            # add s3 lifecycle policy
            s3Client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "basic-rule",
                            "Filter": {
                                "Prefix": ""
                            },
                            "Status": "Enabled",
                            "Transitions": [
                                {
                                    "Days": 1,
                                    "StorageClass": "INTELLIGENT_TIERING"
                                }
                            ]
                        }
                    ]
                }
            )


def create_lambda_if_not_exists(function_name, iam_role_arn, layer_arn, bucket_name):
    try:
        response = lambdaClient.get_function(FunctionName=function_name)
        lambda_arn = response['Configuration']['FunctionArn']
        print(f'Lambda function already exist: {function_name}')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f'Creating Lambda function: {function_name}')
            
            zip_file = zipfile.ZipFile('artifacts/data_processing_lambda.zip', 'w')
            zip_file.write('./data_processing_lambda.py', compress_type=zipfile.ZIP_DEFLATED)
            zip_file.close()
            
            # create lambda function
            response = lambdaClient.create_function(
                FunctionName=function_name,
                Handler='data_processing_lambda.lambda_handler',
                Runtime='python3.7',
                Role=iam_role_arn,
                Code={
                    'ZipFile': open('artifacts/data_processing_lambda.zip', 'rb').read()
                },
                Timeout=300,
                MemorySize=128,
                Layers=[
                    layer_arn,
                ]
            )
            time.sleep(15)
            lambda_arn = response['FunctionArn']

            lambda_state_active = True

            # check lambda function status
            while lambda_state_active:
                response = lambdaClient.get_function(FunctionName=function_name)
                lambda_state = response['Configuration']['State']
                print("lambda function: {}".format(lambda_state))
                if lambda_state != 'Active':
                    print("Processing Lambda State: {} ".format(lambda_state))
                    lambda_state_active = True
                else:
                    print("Processing Lambda: {} is available now ".format(function_name))
                    lambda_state_active = False
                time.sleep(15)

            # add lambda permission on s3 bucket
            lambdaClient.add_permission(
                FunctionName=function_name,
                StatementId='{}-invoke-permission'.format(function_name),
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{bucket_name}')

            bucket_notification = s3.BucketNotification(bucket_name)

            # add lambda bucket notification for any new object added to s3 bucket on prefix 'input'
            bucket_notification.put(
                NotificationConfiguration={
                    'LambdaFunctionConfigurations': [
                        {
                            'LambdaFunctionArn': lambda_arn,
                            'Events': ['s3:ObjectCreated:*'],
                            'Filter': {
                                'Key': {
                                    'FilterRules': [
                                        {
                                            'Name': 'prefix',
                                            'Value': 'input/'
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            )

        else:
            print(e.response['Error']['Code'])
            print("Unexpected error: %s" % e)
            print("Exiting ...")
            exit(1)
            
            return lambda_arn


def upload_file_to_s3_bucket(bucket_name, filename):
    try:
        s3Client.head_bucket(Bucket=bucket_name)
        print(f'Uploading data file: {filename}')
        wr.s3.upload(local_file=filename, path=f's3://{bucket_name}/{filename}')
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404' or error_code == 'NoSuchBucket':
            print(f'S3 bucket does not exist: {bucket_name}')
    
    
def main():
    bucket_name = 'adobe-hit-level-data-storage'
    function_name = 'data_processing_serverless_lambda'
    iam_role_arn = 'arn:aws:iam::457180471489:role/lambda_execution_role'
    layer_arn = 'arn:aws:lambda:us-east-1:457180471489:layer:data_engg_py37_layer:1'
    filename = 'input/data.tsv'
    
    # create s3 bucket for hit level data storage
    create_s3_bucket_if_not_exists(bucket_name)
    
    # create lambda for serverless deployment of the app
    create_lambda_if_not_exists(function_name, iam_role_arn, layer_arn, bucket_name)

    # upload the input file to the s3 bucket
    upload_file_to_s3_bucket(bucket_name, filename)
    
    
if __name__ == "__main__":
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ BEGIN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    main()
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ END ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
