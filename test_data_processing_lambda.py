import json
import unittest
from moto import mock_s3
import boto3
import awswrangler as wr

with open('config.json') as f:
    config_env = json.load(f)

bucket_name = config_env['s3_bucket_name']
filename = config_env['s3_filename']

@mock_s3
class TestAdobeAnalytics(unittest.TestCase):
    def setUp(self):
        # s3 setup for unit test
        self.s3Client = boto3.client('s3')
        self.s3_bucket = self.s3Client.create_bucket(Bucket=bucket_name)
        wr.s3.upload(local_file=filename, path=f's3://{bucket_name}/{filename}')

    def test_calculate_revenue(self):
        from data_processing_lambda import AdobeAnalytics
        AdobeAnalytics(bucket_name, filename).calculate_revenue()

if __name__ == '__main__':
    unittest.main()
