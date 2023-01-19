#Azure function template for long-running apps (= longer then default timeout)
#A usefull pattern to both avoiding timeout issues and being able to check on calculation status
#Below also included a simplified(!) method to interact with db to record http response
#for production environments more safety checks recommended on db side


import logging
import azure.function as func
import time
import asyncio
import pyodbc

import function_template #import any function to be executed inside function app


async def main(req: func.HttpRequest) -> func.HttpResponse:
    
     # extend parameters if necessary
    logging.info('Python HTTP trigger feature processed a request.')
    feature_name = req.params.get('feature_name')
    campaign_id = req.params.get('campaign_id')
    user_id = req.params.get('user_id')
    session_id = req.params.get('session_id')



    read_write_voyager = Write_update_db()

    
    #check if all necessary paramaters are present in request, otherwise return response
    if feature_name == 'main_algorithm':
        if not user_id:
            return func.HttpResponse('user ID not included in http call')
        if not session_id:
            return func.HttpResponse('session ID not included in http call')
        if not campaign_id:
            return func.HttpResponse('campaign ID not included in http call')


    #create task asynchronously https://learn.microsoft.com/en-us/azure/azure-functions/python-scale-performance-reference
    #response should be recorded to a db: 200 = request receied to func app, 202 = success (inside t), 500 = failure
               
        asyncio.create_task(read_write_voyager.write_http_status(session_id = str(session_id), time =  int(time.time() , code = '200')))

        try:
            asyncio.create_task(function_template(user_id, session_id, campaign_id))
        except:
            asyncio.create_task(read_write_voyager.update_http_status(session_id = str(session_id), time = int(time.time() , code = '500')))

    else:
        return func.HttpResponse('feature not implemented yet')


##########################################################################

# preferably import below Write_update_db to above


class Write_update_db():
    def __init__(self):
        self.server, self.db, self.user, self.pw, self.driver = 'Servername', 'DB_name', 'Username', 'Password', '{ODBC Driver for SQL Server}'
        self.cnxn = pyodbc.connect('DRIVER=' + self.driver + ';SERVER=' + self.server + '; DATABASE=' + self.db_name + ';UID=' + self.user + ';PWD=' + self.password)
        
    async def write_http_status(self, session_id, time, code):
        self.cnxn.execute_sql(f"insert into [schema].[app_response] values ( {time}, {code}, {session_id}) ")
        self.cnxn.commit()
        return
     
    async def update_http_status(self, session_id, time, code):
        self.cnxn.execute_sql(f"update [schema].[app_response] set TimeInserted = {time}, Status = {code} where Session = {session_id} ")
        self.cnxn.commit()
        return



 

