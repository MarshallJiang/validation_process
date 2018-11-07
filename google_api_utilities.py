from googleapiclient.discovery import build
from httplib2 import Http
from apiclient import errors
from oauth2client import file, client, tools
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pandas as pd
import datetime
import re
import io
import base64
import pymysql
import json
import requests
import os
import smtplib  
import mimetypes


SHEET_TOKEN_DIR = './credentials/token/sheet_token.json'
MAIL_TOKEN_DIR = './credentials/token/mail_token.json'
SHEETS_SCOPES = [
    'https://www.googleapis.com/auth/drive',
]
MAIL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://mail.google.com/'
]
PROCESS_OWNER = 'marshall@shopback.com'

class gsheetHandler :

    def __init__ (self, CREDENTIAL_DIR) :
        
        store = file.Storage(SHEET_TOKEN_DIR)
        if not os.path.exists(SHEET_TOKEN_DIR[:SHEET_TOKEN_DIR.index('/sheet_token.json')]):
            #save token with successfully client serect authentication.
            os.makedirs(SHEET_TOKEN_DIR[:SHEET_TOKEN_DIR.index('/sheet_token.json')])
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(CREDENTIAL_DIR, SHEETS_SCOPES)
            creds = tools.run_flow(flow, store)
        self.spreadsheet_service = build('sheets', 'v4', http=creds.authorize(Http()))

    def to_DataFrame(self, SPREADSHEET_ID, RANGE_NAME, COLUMNS_INDEX=0, NA_VALUES='') : # google sheet api return '' for null cell value.

        try :
            result = self.spreadsheet_service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
            values = result.get('values', [])
            '''
            dataframe return logic :
            - assume the row start of RANGE_NAME is sensible which can properly handle spreadsheet's column.
            - get the longest list and make rest of lists extend into the same length as the longest one.
            '''
            longest_list = max(values, key=len)
            for row in values :
                row += ([NA_VALUES] * (len(longest_list) - len(row)))
            self.gsheet_DataFrame = pd.DataFrame(values[COLUMNS_INDEX + 1:], columns=values[COLUMNS_INDEX])
            self.gsheet_DataFrame = self.gsheet_DataFrame.dropna(how='all')
            return self.gsheet_DataFrame
        except (errors.HttpError, error):
            print ('An error occurred: %s' % error)
    
    def get_sheetId(self, SPREADSHEET_ID, RANGE_NAME) :
        response = self.spreadsheet_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID, ranges=RANGE_NAME, fields='sheets.properties.sheetId').execute()
        return response['sheets'][0]['properties']['sheetId']

    def update_Spreadsheet(self, SPREADSHEET_ID, BODY) :
        response = self.spreadsheet_service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=BODY).execute()
        return response
    
    def update_Spreadsheet_format(self, SPREADSHEET_ID, BODY) :
        response = self.spreadsheet_service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=BODY).execute()
        return response

class gmailHandler :

    def __init__(self, CREDENTIAL_DIR) :

        store = file.Storage(MAIL_TOKEN_DIR)
        if not os.path.exists(MAIL_TOKEN_DIR[:MAIL_TOKEN_DIR.index('/mail_token.json')]):
            os.makedirs(MAIL_TOKEN_DIR[:MAIL_TOKEN_DIR.index('/mail_token.json')])
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(CREDENTIAL_DIR, MAIL_SCOPES)
            creds = tools.run_flow(flow, store)
        self.mail_service = build('gmail', 'v1', http=creds.authorize(Http()))

    def send_Mail(self, message_plain_text, to, sender='me', subject=None, attachments=None, reply_message_id=None) :
        '''
        Usage - 
        @send the mail to given address and attachments.
        Args - 
        @message_plain_text : the plain text body of the mail.
        @to : mail address that going to be delivered.
        @sender : default as `me`.
        @subject : the subject of the mail, when reply_message_id has been given, will auto substitute into that reply threads subject.
        @attachments : list of attachments that will going to be sent.
        @reply_message_id : id of the message that going to be replied.
        Return -
        @return the MIME object.
        '''
        message = MIMEMultipart() 
        message['to'] = ' '.join(to)
        message['from'] = PROCESS_OWNER
        if reply_message_id : 
            metadata = self.mail_service.users().messages().get(userId='me', id=reply_message_id, format='full').execute()
            threadId = metadata['threadId']
            message['In-Reply-To'] = reply_message_id
            message['References'] = reply_message_id
            for payload in metadata['payload']['headers'] : 
                
                if payload['name'] == 'Subject' :
                    message['Subject'] = payload['value']
                
                else :
                    continue
        else :
            if not subject :
                print('''no subject provided for MIME''')
                return False
            else :
                message['subject'] = subject
            
        message.attach(MIMEText(message_plain_text, 'plain'))

        for attachment in attachments : 
            if attachment :
                my_mimetype, encoding = mimetypes.guess_type(attachment)

                if my_mimetype is None or encoding is not None:
                    my_mimetype = 'application/octet-stream' 
                main_type, sub_type = my_mimetype.split('/', 1)

                if main_type == 'text':
                    temp = open(attachment, 'r')  
                    attachement = MIMEText(temp.read(), _subtype=sub_type)
                    temp.close()
                elif main_type == 'image':
                    temp = open(attachment, 'rb')
                    attachement = MIMEImage(temp.read(), _subtype=sub_type)
                    temp.close()
                    
                elif main_type == 'audio':
                    temp = open(attachment, 'rb')
                    attachement = MIMEAudio(temp.read(), _subtype=sub_type)
                    temp.close()   

                elif main_type == 'application' and sub_type == 'pdf':   
                    temp = open(attachment, 'rb')
                    attachement = MIMEApplication(temp.read(), _subtype=sub_type)
                    temp.close()

                else:              
                    attachement = MIMEBase(main_type, sub_type)
                    temp = open(attachment, 'rb')
                    attachement.set_payload(temp.read())
                    temp.close()

                encoders.encode_base64(attachement) 
                filename = os.path.basename(attachment)
                attachement.add_header('Content-Disposition', 'attachment', filename=filename) # name preview in email
                message.attach(attachement) 
        print(message)
        ## Part 4 encode the message (the message should be in bytes)
        message_as_bytes = message.as_bytes() # the message should converted from string to bytes.
        message_as_base64 = base64.urlsafe_b64encode(message_as_bytes) #encode in base64 (printable letters coding)
        raw = message_as_base64.decode()  # need to JSON serializable (no idea what does it means)
        message =  {'raw': raw}
        if reply_message_id :
            message['threadId'] = threadId
        try :
            message = self.mail_service.users().messages().send(userId=sender, body=message).execute()
            return message

        except (errors.HttpError):
            print ('An error occurred: %s' % errors.HttpError)


    def filter_Mails(self, user_id='me', from_user=None, to_user= None, epoch_after=None, epoch_before=None) :
        
        '''
        Usage -
        @to filter threads which meet the given argument as criteria.
        Args -  
        @user_id : default as `me`.
        @from_user : filter out the sender
        @to_user : filter out the receiver.
        @epoch_after : filter out the mail locate after specific time range YYYY-MM-DD HH:MM:SS
        @epoch_before : filter out the mail locate before specific time range YYYY-MM-DD HH:MM:SS
        Return - 
        @Return list of dictionaries with snippet detail included.
        '''
        query = ''

        if from_user :

            query += 'from:%s '%from_user

        if to_user :
            query += 'to:%s '%to_user

        if epoch_after :
            query += 'after:%s '%datetime.datetime.strptime(epoch_after, '%Y-%m-%d %H:%M:%S').strftime('%s')

        if epoch_before :
            query += 'before:%s '%datetime.datetime.strptime(epoch_before, '%Y-%m-%d %H:%M:%S').strftime('%s')

        try:
            response = self.mail_service.users().messages().list(userId=user_id,q=query).execute()
            messages = []

            if 'messages' in response:
                messages.extend(response['messages'])

            while 'nextPageToken' in response:
                page_token = response['nextPageToken']
                response = self.mail_service.users().messages().list(userId=user_id, q=query,pageToken=page_token).execute()
                messages.extend(response['messages'])

            return messages          
        except errors.HttpError:
            print ('An error occurred: %s'%errors.HttpError)

    def get_attachment(self, message_id, user_id='me', filename_regex=None, store_dir=None) :
        '''
        Usage : 
        @Get the attachments by the given message_id of threads.
        Args :
        @message_id : id of the threads to get attachments.
        @user_id : default as `me`.
        @filename_regex : to filter file which their name match the regex.
        @store_dir : directory of the file is going to be saved.
        Return :
        @this function is build as an generator to yield every file within the thread.
        '''

        message = self.mail_service.users().messages().get(userId=user_id, id=message_id).execute()
        internalDate = message['internalDate']
        parts = [message['payload']]
        while parts:
            part = parts.pop()
            if part.get('parts') :
                parts.extend(part['parts'])
            if part.get('filename') :
                if filename_regex :
                    pattern = re.compile(filename_regex, re.UNICODE)           
                    if pattern.match(part['filename'].lower()) is None : 
                        continue
                if 'data' in part['body'] :
                    file_data = base64.urlsafe_b64decode(part['body']['data'].encode('utf-8'))
                elif 'attachmentId' in part['body'] :
                    attachment = self.mail_service.users().messages().attachments().get(userId=user_id, messageId=message_id, id=part['body']['attachmentId']).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('utf-8'))
                else :
                    continue
                if store_dir == None :
                    path = None
                else :
                    path = ''.join([store_dir, part['filename']])
                    f = open(path, 'wb')
                    f.write(file_data)
                    f.close()
                yield {
                    'messageId' : message_id,
                    'internalDate' : internalDate,
                    'file' : path
                }

            
