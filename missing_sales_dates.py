#Original script was created to create a simple list of stores with missing 
# sales dates (inventory, traffic etc.) for a simple Power BI model that could
# be used by the shareholders to notify stores they had missing data. 
#(For internal purposes more complex are integrity checks are reccommended of course)



import pyodbc
import pandas as pd
from datetime import datetime
from datetime import timedelta
import glob
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import ClientSecretCredential

# server credentials
server = '***server***.database.windows.net'
database = '***database***'
username = '***username***'
password = '***password***'
driver= '{ODBC Driver *** for SQL Server}'

# data lake credentials
storage_url = "https://***storage_url***.blob.core.windows.net/" 
tenant_id = "***tenant_id***"
client_id = "***client_id***"
client_secret = "***client_secret***"
connect_str = 'DefaultEndpointsProtocol=https;AccountName=***xxx***;AccountKey=***yyy***;EndpointSuffix=***core.windows.net'

# test variable (if set to True, debug print)
test = False

###############################################################################################################

def upload_to_blob(file, container, dir, blob, test):

    credential = ClientSecretCredential(tenant_id = tenant_id, client_id = client_id, client_secret = client_secret )
    
    #blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    #container_client = blob_service_client.get_container_client(f"{container}")

    container_name_up = f"{container}/{dir}/"


    blob_client_up = BlobClient(storage_url, container_name= container_name_up, blob_name=  f"{blob}.csv", credential=credential)
    
    try:
        blob_client_up.upload_blob(file.to_csv(index=False), overwrite=True)

        if test is True:
            print("Upload done")

    except Exception as err:

        if test is True:
            print("Upload failed: " + err)


def open_connection(test):

    try:
        connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        
        global cnxn
        cnxn = connection
        if test is True:
            print("Connection to database is open - ")
    except:
        if test is True:
            print("connection to database couldn't be established")


    
def close_connection(test):

    try:
        cnxn.close()
        
        if test is True:
            print("Connection to database is closed -")
    except:
        if test is True:
            print("connection to database couldn't be closed")
        pass


def df_of_missing_files(partner_list):

    try:
        open_connection(test)

        missing_files = pd.DataFrame() 
        missing_files[['Calendar_Date', 'Location_Code']] =  ''

        for partner in partner_list:

            script = f"select distinct Calendar_Date , Location_Code from [schema].[Inventory_{partner}] where Calendar_Date is not null and Location_Code is not null "
            df= pd.read_sql_query(script, cnxn)

            

            for store_number in df['Location_Code'].unique(): 

                #for all unique location code -> 1) create dataframe with all possbile days since creation to yesterday 2) create a dataframe with all dates that have a sale
                #   by substructing 2) from 1) you get a dataframe with the days with missing sales

                df1 = df.loc[df['Location_Code'] == store_number]

                start_date = min(df1['Calendar_Date'])
                end_date = datetime.today().date() - timedelta(days=1)
                delta = end_date - start_date   
            
                days_all = [start_date + timedelta(days=i)  for i in range(delta.days + 1)]

                df2 = pd.DataFrame(days_all, columns =['Calendar_Date'])
                df2['Location_Code'] = store_number


                df_diff = pd.merge(df1, df2, how='right', indicator='Exist')
                df_diff = df_diff.loc[df_diff['Exist'] == 'right_only']
                df_diff = df_diff[['Calendar_Date', 'Location_Code']]

                missing_files = pd.concat([df_diff , missing_files], ignore_index=True, sort=False)

                return missing_files

    except Exception as Err:
        if test is True:
            print("Error " + Err)


    finally:
        close_connection(test)


if __name__ == '__main__':

    files = df_of_missing_files(['a partner','b partner','c partner'])

    upload_to_blob(files, "container's name", "dir's name", "blob's name", test)

    # Safety measure not to pile up files on long-running VM's
    for f in glob.iglob('*.csv', recursive=True):
                os.remove(f)
