
# Simple script to check if a product image exist and update db table accordingly
# Test is simple: hit product http address with urllib.request.urlretrieve, if there
# is a response then image has picture uploaded. Replacing a much slower script that
# actually downloaded the pictures to see if they exist

#("https://productimages.***.com/Itemimages/***.jpg")


import urllib.request
from datetime import datetime
import pyodbc
import pandas as pd



server = '***server***.database.windows.net'
database = '***database***'
username = '***username***'
password = '***password ***'   
driver= '{ODBC Driver *** for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
script =  "SELECT [Number],[File_Path] FROM [schema].[_mst_ItemUrl] where [Valid_URL]=0 or [ctrl_RowInsertUTCTime] is null"
df= pd.read_sql_query(script, cnxn)

image_url = list(df["File_Path"])

for x in image_url:
    try:
        file_name = x.split('/')[-1]
        urllib.request.urlretrieve(x, file_name) 
        cursor = cnxn.cursor()
        cursor.execute("UPDATE [schema].[Item_url] SET [Valid_URL] = ?, [ctrl_RowInsertUTCTime] = ? WHERE [File Path] = ? ", 1,datetime.now(), x)          
        cnxn.commit()
    except Exception:  
        cursor = cnxn.cursor()
        cursor.execute("UPDATE [schema].[Item_url] SET [Valid_URL] = ?, [ctrl_RowInsertUTCTime] = ? WHERE [File Path] = ? ", 0,datetime.now(), x)           
        cnxn.commit()
        pass
 
