import boto3
from dotenv import dotenv_values
import pprint


#loading aws credentials to authenticate the s3 bucket service
config_object= {
    **dotenv_values('.env')
}


#creating the session / we are making use of the s3 service
session = boto3.Session(
    aws_access_key_id= config_object['ACCESS_KEY'],
    aws_secret_access_key=config_object['SECRET_KEY'],
)

rds_client = session.client('rds', region_name='us-east-2')


if __name__ == '__main__':

    pprint.pprint(rds_client.describe_db_clusters())
