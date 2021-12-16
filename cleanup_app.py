import os
import boto3
from botocore.exceptions import ClientError

s3Client = boto3.client('s3')
s3 = boto3.resource('s3')
lambdaClient = boto3.client('lambda')

def delete_s3_bucket_if_exists(bucket_name):
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        print(f'Deleting S3 bucket: {bucket_name}')
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        s3Client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f'S3 bucket does not exist: {bucket_name}')


def delete_lambda_if_exists(function_name):
    try:
        if os.path.exists("artifacts/data_processing_lambda.zip"):
            os.remove("artifacts/data_processing_lambda.zip")
            
        lambdaClient.get_function(FunctionName=function_name)
        lambdaClient.delete_function(FunctionName=function_name)
        print(f'Deleting lambda: {function_name}')
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("Lambda does not exist")
        else:
            print(e.response['Error']['Code'])
            print("Unexpected error: %s" % e)
            print("Exiting ...")
            exit(1)
        
        
def main():
    bucket_name = 'hit-level-data-storage'
    function_name = 'data_processing_serverless_lambda'
    
    # delete s3 bucket for hit level data storage
    delete_s3_bucket_if_exists(bucket_name)
    
    # delete lambda for serverless deployment
    delete_lambda_if_exists(function_name)
    
if __name__ == "__main__":
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ BEGIN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    main()
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ END ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
