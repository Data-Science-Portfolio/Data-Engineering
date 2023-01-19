
#Script meant to send out email notifications based on files in a 
#data lake. There is one file with a 'master list' and multiple other
#files #each containing a result of a database view + a list of recipients
#The script needs to send all rules to all recipients, and also also a 
#summary of emailed rules back to to the sending department as well as
#send error notification emails when necessary


################################################################IMPORT###############################################################################


import pandas as pd

from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import ClientSecretCredential
import os
import time
import pretty_html_table
from pretty_html_table import build_table
from O365 import Account


############################################################CREDENTIALS#############################################################################


## 0 = no mail only print   >0 = sending testing emails (insert below)   >1 = sending error mails and notifications also   7 = sending mails to CORRECT adresses
test = 0

#insert email for testing for the below variable:
testemail = "***testemail***" 

#insert email for error notifications and summary report for the below variable ( if test NOT = 7 it will use testemail instead):
departmentemail = "***departmentemail***"

#the dedicated account to send out mails
sender = "***senderemail***"


#credentials for downloading/uploading to blob storage -------------------

storage_url = "https://***.blob.core.windows.net/" 

tenant_id = "******"

client_id = "******"

client_secret = "******"

credential = ClientSecretCredential(tenant_id = tenant_id, client_id = client_id, client_secret = client_secret )

#credentials for looping through files in blob storage --------------------

connect_str='DefaultEndpointsProtocol=https;AccountName=***;AccountKey=***;EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client("transientzone")

#credentials for sending emails ------------------------------------------

app_client_id = "******"

tenant_secret = "******"

tenant_id = "******"


############################################################FUNCTIONS################################################################################

def send_email(message, email, email_title, mailtype, attachment = '', tabledata = '', app_client_id = app_client_id, tenant_secret = tenant_secret, /
               tenant_id = tenant_id, sender = sender):

    credentials = (app_client_id, tenant_secret)
    account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id)
    if account.authenticate():
        print('*** Authenticated - Sending mail!')
    m = account.new_message(resource= sender)
    
    if test == 7:
        m.to.add(email)
    else:
        m.to.add(testemail)
    
    
    m.subject = email_title
    
    
    if mailtype == "notification":

        html_body = """
        <html>
        <head>
        </head>

        <body>

        <p>
        """ + df['ViewName'][x] + """
        </p>

        <p>
        """ + (df['RuleDescription'][x]) + """
        </p>

        <p>
                {0}
        </p>

        </body>

        </html>
        """.format(build_table(tabledata, 'blue_light', font_size='small',width='auto'))

        m.body = html_body
        m.attachments.add(attachment)

    elif mailtype == "error":
        m.body = message
    
    # when sending out mails we set up a timer also so we don't send more messages/minute than the limit
    if test > 0:
            time.sleep(3)
            m.send()



def main(test): #Send out emails per file (=per rule) to each recepients


    #Loop through all files in transientzone/email_notification/master/ and open "master_list"
    #(there should be no other files in that folder ideally)
    blob_list_inSP = container_client.list_blobs(name_starts_with="email_notification/master/")

    for blobSP in blob_list_inSP:
        if blobSP.name.split("/")[-1] == "master_list":

            try:
                #Download "master_list" and read it into a dataframe
                current_fileSP = blobSP.name.split("/")[-1]

                blob_clientDownSP = BlobClient(storage_url, container_name="transientzone/email_notification/master/", /
                                               blob_name= current_fileSP, credential=credential)

                with open(current_fileSP, "wb") as f:
                    blob_data = blob_clientDownSP.download_blob()
                    blob_data.readinto(f)
                    f.close()

                df = pd.read_csv(current_fileSP, delimiter = "|", header=0,  index_col=False)
    
                
                #Change all NULL emails to department email in "master_list" and in case of multiple emails put them into a list instead of string
                for x in range(len(df['Mail'])) :
                    if str(df['Mail'][x]) == 'nan':
                        df.loc[x, 'Mail'] = '***department_email***'
            
                    try:
                        df.at[x, 'Mail'] = df['Mail'][x].split(";")
                    except:
                        pass
                    
                
                #Looping through active email_notifications in "master_list"
                for x in range(len(df)):
                    if df['Active'][x] == 1:
                        
                        content_file_name = str(df['ViewName'][x])[4:]
                        print(content_file_name)

                        #Reading individual Email_notification views to a dataframe 
                        blob_clientDownContent = BlobClient(storage_url, container_name="transientzone/email_notification/contents/", /
                                                            blob_name= content_file_name , credential=credential)

                        with open(content_file_name, "wb") as f:
                            blob_data = blob_clientDownContent.download_blob()
                            blob_data.readinto(f)
                            f.close()

                        dfcontent  = pd.read_csv(content_file_name)
                        
                        # Remove email_notification view from directory once we are done working with it 
                        #(!if you don't delete you will have a file for each email_notification in directory)    
                        os.remove(content_file_name)

                        #If dataframe is empty (no errors for email_notification) jump to next email_notification
                        if dfcontent.empty:
                            continue
                            
                        dfcontent.to_excel("attachment.xlsx")
                        
                            
                        ### sending out 'Normal Email_notifications'  - reading recipient from email_notification validate table (df) directly 
                        for y in range(len(df['Mail'][x])):
                            
                            
                            attachment = str(os.path.abspath(os.getcwd())) +  "\\attachment.xlsx"
                            message = "test"
                            email = str(df['Mail'][x][y])
                            email_title = df['MailSubject'][x]
                            mailtype = "notification"
                            tabledata = dfcontent
                            
                            send_email(message, email, email_title, mailtype, attachment, tabledata)
                            
                            #For testing
                            print("*** " + str(df['Mail'][x][y]))
                            print("*** " + str(df['MailSubject'][x])) 
                            
                            
                            
                        ### sending out 'Recalled Email_notification's  - 
                        #reading recipient from the email_notification views to send both emails and relevant part of dataframe at store level 

                        # Find column name that contains emails
                        if 'Recalled' in str(df['RuleDescription'][x]):
                            for col in dfcontent.columns:
                                if "@" in str(dfcontent[col][0]):
                                    emailcolumn = str(col)
                                    
                            # Order by column containing email
                            dfcontent = dfcontent.sort_values(by=emailcolumn, ascending=True, na_position='last')

                            # Send out respective parts of the dataframe only to the store email adress
                            for ie in dfcontent[emailcolumn].unique():
                                df_by_email = dfcontent[dfcontent[emailcolumn] == ie]
                                df_by_email.to_excel("attachment.xlsx")
                                attachment = str(os.path.abspath(os.getcwd())) +  "\\attachment.xlsx"
                                
                                message = "test" 
                                email = ie
                                email_title = df['MailSubject'][x]
                                mailtype = "notification"
                                tabledata = df_by_email
                                
                                send_email(message, email, email_title, mailtype, attachment, tabledata)
                                
                                #For testing
                                print("*** " + ie)
                                print("*** " + str(df['MailSubject'][x]))   
                                
                            
                    else:
                        pass
    
            
                    
            except BaseException as Err:
                # Mailing a notification in case we encounter any problems
                
                #For testing
                print(Err)
                
                if test > 1:
                
                    message = 'An unexpected error occured while processing '+ content_file_name + ': ' + str(Err)
                    email = departmentemail
                    email_title = 'Email_notification Error'
                    mailtype = "error"

                    
                    send_email(message, email, email_title, mailtype)
                    
                pass
            
            finally:
                # Summary if email_notifications sent out
                print("Task complete")
                if test > 1:
                    message = 'Under construction'
                    email = departmentemail
                    email_title = 'Email_notification Summary (not done yet)'
                    mailtype = "error"
 

                    send_email(message, email, email_title, mailtype)
                    
                # Clean up directory
                os.remove("attachment.xlsx")
                os.remove("master_list")
                try:
                    os.remove("o365_token.txt")
                except:
                    pass
            
        else:
            pass
        
    else: 
        # Notification in case validate output table file is missing
        if test > 1:
            message = 'Email_notification error: transientzone/email_notification/master/master_list file is missing!'
            email = departmentemail
            email_title = 'Email_notification error: master_list file is missing'
            mailtype = "error"


            send_email(message, email, email_title, mailtype)

        # For testing
        print('transientzone/email_notification/master/master_list file is missing!')


if __name__ == '__main__':
    main(test)
