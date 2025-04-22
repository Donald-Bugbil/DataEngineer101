from dotenv import dotenv_values
from rich.console import Console
from rich.table import Table
import pprint
import boto3
import os
import csv
import sqlite3



console = Console()


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

database_schema = {
 'Industry_aggregation_NZSIOC': 'Industry_aggregation_NZSIOC',
 'Industry_code_ANZSIC06': 'Industry_code_ANZSIC06',
 'Industry_code_NZSIOC': 'Industry_code_NZSIOC',
 'Industry_name_NZSIOC': 'Industry_name_NZSIOC',
 'Units': 'Units',
 'Value': 'Value',
 'Variable_category': 'Variable_category',
 'Variable_code': 'Variable_code',
 'Variable_name': 'Variable_name',
 'Year': 'Year'

 }

#returns table
def table_generator():

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column(database_schema['Industry_aggregation_NZSIOC'], style="dim", width=12)
    table.add_column(database_schema['Industry_code_ANZSIC06'], style="green")
    table.add_column(database_schema['Industry_code_NZSIOC'], style="cyan")
    table.add_column(database_schema['Industry_name_NZSIOC'], style="red")
    table.add_column(database_schema['Units'],style="yellow")
    table.add_column(database_schema['Value'], style="blue")
    table.add_column(database_schema['Variable_category'])
    table.add_column(database_schema['Variable_code'])
    table.add_column(database_schema['Variable_name'])
    table.add_column(database_schema['Year'])


    return table



    pass

def database_table_setup():
    try:

        connection = sqlite3.connect("enterprise.db")
        cursor = connection.cursor()


        cursor.execute(f"CREATE TABLE enterprise({database_schema['Industry_aggregation_NZSIOC']}, {database_schema['Industry_code_ANZSIC06']}, {database_schema['Industry_code_NZSIOC']}, {database_schema['Industry_name_NZSIOC']}, {database_schema['Units']}, {database_schema['Value']}, {database_schema['Variable_category']}, {database_schema['Variable_code']}, {database_schema['Variable_name']}, {database_schema['Year']})")
        res = cursor.execute("SELECT name FROM sqlite_master WHERE name='enterprise'")
        print(res.fetchone())
    except Exception as e :
        
        print(f"an exception occured {e}")




cloud_file_name = 'annual_enterprise.csv'


def select_records():

    table = table_generator()

    connection= sqlite3.connect('enterprise.db')
    cursor = connection.cursor()

    query = "SELECT * FROM enterprise"

    response = cursor.execute(query)

    sample_records = response.fetchmany(500)

    for record in sample_records:
        table.add_row(
            record[0],
            record[1],
            record[2],
            record[3],
            record[4],
            record[5],
            record[6],
            record[7],
            record[8],
            record[9]
        )
    
    #print(sample_records)
    
    console.print(table)
    




# I'm going ahead to define a new function that read the csv file to help with reusability
def read_csv():

    connection = sqlite3.connect("enterprise.db")
    cursor = connection.cursor()

    with open('local_file') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')

        for row in csv_reader:
            Industry_aggregation_NZSIOC = row['Industry_aggregation_NZSIOC']
            Industry_code_ANZSIC06 = row['Industry_code_ANZSIC06']
            Industry_code_NZSIOC = row['Industry_code_NZSIOC']
            Industry_name_NZSIOC = row['Industry_name_NZSIOC']
            Units = row['Units']
            Value = row['Value']
            Variable_category = row['Variable_category']
            Variable_code = row['Variable_code']
            Variable_name = row['Variable_name']
            Year = row['Year']




            print(row['Industry_aggregation_NZSIOC'])
            cursor.execute(
                f"INSERT INTO enterprise ({database_schema['Industry_aggregation_NZSIOC']}, {database_schema['Industry_code_ANZSIC06']}, {database_schema['Industry_code_NZSIOC']}, {database_schema['Industry_name_NZSIOC']}, {database_schema['Units']}, {database_schema['Value']}, {database_schema['Variable_category']},{database_schema['Variable_code']}, {database_schema['Variable_name']}, {database_schema['Year']}) VALUES (?,?,?,?,?,?,?,?,?,?)", (row['Industry_aggregation_NZSIOC'], row['Industry_code_ANZSIC06'], row['Industry_code_NZSIOC'], row['Industry_name_NZSIOC'], row['Units'], row['Value'], row['Variable_category'], row['Variable_code'],row['Variable_name'], row['Year'])

                )       
            connection.commit()





if __name__ == '__main__':

    # 1
    #s3_client.download_file(config_object['BUCKET_NAME'], cloud_file_name, 'local_file')

    #scenario for displaying all items in the bucket
    # for bucket in s3_client.buckets.all():
    #     print(bucket.name)
    #database_table_setup()

    #read_csv()

    select_records()

