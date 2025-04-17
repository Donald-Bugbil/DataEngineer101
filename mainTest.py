import boto3
import os
from dotenv import dotenv_values

#secret keys, exposed and very harmful

#loading aws credentials to authenticate the s3 bucket service
config_object= {
    **dotenv_values('.env')
}


#creating the session / we are making use of the s3 service
session = boto3.Session(
    aws_access_key_id= config_object['ACCESS_KEY'],
    aws_secret_access_key=config_object['SECRET_KEY'],
)

cloud_file_name = 'annual_enterprise.csv'


session = boto3.Session(
    aws_access_key_id=config_object['ACCESS_KEY'],
    aws_secret_access_key=config_object['SECRET_KEY']
)


s3_client = session.client('s3')



if __name__ == '__main__':

    with open('local_test', 'wb') as file:
        s3_client.download_file(config_object['BUCKET_NAME'], cloud_file_name, file)

