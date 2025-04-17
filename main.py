from dotenv import dotenv_values
import boto3
import os
import csv



#loading aws credentials to authenticate the s3 bucket service
config_object= {
    **dotenv_values('.env')
}


#creating the session / we are making use of the s3 service
session = boto3.Session(
    aws_access_key_id= config_object['ACCESS_KEY'],
    aws_secret_access_key=config_object['SECRET_KEY'],
)

#create service
s3_client = session.client('s3')

#downloading our file in the bucket can be done in two ways
# 1 - download directly
# 2 - create a file locally and write the file from the bucket to the local file

cloud_file_name = 'annual_enterprise.csv'


# I'm going ahead to define a new function that read the csv file to help with reusability
def read_csv():

    with open('local_file') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        
        for row in csv_reader:
            print(row)





if __name__ == '__main__':

    # 1
    #s3_client.download_file(config_object['BUCKET_NAME'], cloud_file_name, 'local_file')

    #scenario for displaying all items in the bucket
    # for bucket in s3_client.buckets.all():
    #     print(bucket.name)

    read_csv()

