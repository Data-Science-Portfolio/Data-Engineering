#scraping and transforming data from office 365 and storing it in Azure Data Lake Gen 2
#below script can run daily or even multiple times a day if needed


from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from azure.storage.blob import BlobClient
from azure.identity import ClientSecretCredential 

import json
import pandas as pd
import glob
import os
from datetime import datetime
from datetime import timedelta

site_url = 'https://****.sharepoint.com/sites/****/'
app_principal = {
  'client_id': '***client_id***',
  'client_secret': '***client_secret****',
}

context_auth = AuthenticationContext(url = site_url)
context_auth.acquire_token_for_app(client_id = app_principal['client_id'], client_secret = app_principal['client_secret'])
ctx = ClientContext(site_url, context_auth)

#time delta - used below 
date_format_str = "%Y-%m-%dT%H:%M:%SZ"
hours = 2  

#Connect to Supplier Contract List
list = ctx.web.lists.get_by_id('***id***')
items = list.get_items()
ctx.load(items)
ctx.execute_query()


#Look up function to fetch Incoterm
def IncotermGetLookupValue(id):

    list = ctx.web.lists.get_by_title("Incoterms 2020")
    items = list.get_items()
    ctx.load(items)
    ctx.execute_query()

    return "" if id is None else "".join((x.properties["Title"] for x in items if x.properties["ID"] == id)) #assumes no duplicates

#Look up function to fetch Payterm
def PaytermGetLookupValue(id):

    list = ctx.web.lists.get_by_title("Payment Term")
    items = list.get_items()
    ctx.load(items)
    ctx.execute_query()

    return "" if id is None else "".join((x.properties["Key"] for x in items if x.properties["ID"] == id)) #assumes no duplicates

#Look up function to fetch User Details
def UserInfoLookupValue(id):
    
    users = ctx.web.siteUsers.filter("Id eq id")
    ctx.load(users)
    ctx.execute_query()

    return "".join((x.properties["Email"] for x in users if x.properties["Id"] == id)) #assumes no duplicates

def ConverttoInt(id):

    return 1 if (id =='True' or id !='N/A') else 0

def CheckNoneandDateCast(id):
   
    return id if id == None else datetime.strptime(id,date_format_str) + timedelta(hours=n)


#Loop through supplier contract list items and append to an array
dataList = []
for item in items:
     dataList.append(   {"ID":item.properties["ID"],
                         "ParentContract":item.properties["ParentContractId"],
                         "SupplierNumber":item.properties["Title"],
                         "SupplierName":item.properties["SupplierName"],
                         "ProcurementOwner":UserInfoLookupValue(item.properties["ProcurementOwnerId"]),
                         "ContractStart":datetime.strptime(item.properties["ContractStart"], date_format_str) + timedelta(hours=n),
                         "ContractEnd":datetime.strptime(item.properties["ContractEnd"], date_format_str) + timedelta(hours=n),
                         "RenewalDeadline":CheckNoneandDateCast(item.properties["RenewalDeadline"]),
                         "TerminationNotice (months)":item.properties["TerminationNotice"],
                         "TerminationNoticeSupplier (months)":item.properties["TerminationNoticeSupplier"],
                         "Incoterm":IncotermGetLookupValue(item.properties["IncotermId"]),
                         "Payterm":PaytermGetLookupValue(item.properties["PayTermId"]),
                         "PayTermStart":datetime.strptime(item.properties["PayTermStart"], date_format_str) + timedelta(hours=n),
                         "PayTermEnd":datetime.strptime(item.properties["PayTermEnd"], date_format_str) + timedelta(hours=n),
                         "CreditLimit":item.properties["CreditLimit"],
                         "CreditLimitCurrency":item.properties["CreditLimitCurrency"],
                         "MutuallySigned":ConverttoInt(item.properties["MutuallySigned"]),
                         "Comment":item.properties["Comment"],
                         "Attachments":ConverttoInt(item.properties["Attachments"]),
                         "ValidRecord":ConverttoInt(item.properties["ValidRecord"]),
                         "EnableAlert":int(item.properties["EnableAlert"]),
                         "Modified":item.properties["Modified"],
                         "ModifiedBy":UserInfoLookupValue(item.properties["EditorId"]),
                         "Created":item.properties["Created"],
                         "CreatedBy":UserInfoLookupValue(item.properties["AuthorId"])
                         })

#Convert to csv
csvfilename = 'name.csv'
pd.read_json(json.dumps(dataList,default=str)).to_csv(csvfilename, index = None, header=True)


storage_url = "https://***.blob.core.windows.net/" 
tenant_id = '***tenant_id***'
client_id = '***client_id***'
client_secret = '***client_secret***'

container = '***container***'
blob = '**blob****'


# Upload a file to datalake
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

blob_client = BlobClient(storage_url, container_name = container, blob_name = blob, credential=credential) 

with open(f"./{csvfilename}", "rb") as data:
    blob_client.upload_blob(data,overwrite=True)

#solution used to run on a VM with limited storage, so clean up necessary
for f in glob.iglob('*.csv', recursive=True):
    os.remove(f)





