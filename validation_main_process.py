from google_api_utilities import gmailHandler, gsheetHandler
from datetime import datetime, timedelta
from dateutil.parser import parse
from numpy import nan
import pandas as pd
import fileX
import boto3
import json
import os
import io


class single_process :

    def __init__(self, f) :
        self.__dict__ = f

class instance(gmailHandler, gsheetHandler) : 

    def __init__(self, f) :
        self.__dict__ = f
        
        if 'gmail' in self.credentials :
            self.gmail_service = gmailHandler(self.credentials['gmail']['client_secret'])
        if 'gsheet' in self.credentials :
            self.gsheet_service = gsheetHandler(self.credentials['gsheet']['client_secret'])
        if 'boto3' in self.credentials :
            self.s3 = boto3.Session(aws_access_key_id=self.credentials['boto3']['aws_access_key_id'],aws_secret_access_key=self.credentials['boto3']['aws_secret_access_key']).resource('s3')
        self.getFields = self.configuration['const']['getValueFields']
        self.updateFields = self.configuration['const']['updateValueFields']
        self.handleframe = self.gsheet_service.to_DataFrame(self.configuration['const']['sheetId'], self.configuration['const']['sheetName'])

    def process_container(self, single_process) :
        '''
        Usage - 
        @process validation via configuration.

        Args -
        @single_process : process defined by configuration in json.

        Return - 
        @No Return
        '''
        for index, row in self.handleframe.iterrows() : #iter rows in configuration sheet with defined index.

            name = row[self.getFields['name']]
            email = row[self.getFields['email']]
            offerId = row[self.getFields['offerId']]
            sub_used = row[self.getFields['sub_used']]
            data_level = row[self.getFields['data_level']]
            processed_flag = row[self.getFields['processed_flag']]
            filename_regex = row[self.getFields['filename_regex']]
            attach_id = row[self.getFields['thresholdId']]
            result_confirm = row[self.getFields['result_confirm']]
            filename = row[self.getFields['filename']]

            if offerId != '' and email != '' :
                queryTime = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S')
                if processed_flag not in ('', 'error.') :
                    queryTime = (parse(processed_flag, fuzzy_with_tokens=True)[0] + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                if single_process.type == 'send' :
                    from_user = single_process.from_user
                    to_user = email
                elif single_process.type == 'reply' :
                    from_user = email
                    to_user = single_process.to_user
                elif single_process.type == 'validate' : 
                    if result_confirm.lower() == 'ok' : 
                        process_file = self.s3.Object(self.credentials['boto3']['bucket'], filename)
                        v = fileX.Vursor(offerId, io.BytesIO(process_file.get()['Body'].read()))
                        v.process(Data_Level=data_level, Sub_Used=sub_used, s3_Object=True)
                        file = v.write(single_process.store_dir)['report']
                        response = self.gmail_service.send_Mail(
                            single_process.mail_body%v.temp_file['information']['CommissionTotal_estimated'], 
                            single_process.cc, 
                            attachments = [file],
                            reply_message_id = attach_id
                            )
                        self.handleframe.loc[self.handleframe[self.getFields['offerId']] == offerId, self.getFields['result_confirm']] == 'finished.'
                        continue
                    else :
                        continue
                filter_mails = self.gmail_service.filter_Mails(from_user=from_user, to_user=to_user, epoch_after=queryTime)
                for filter_mail in filter_mails : #iter mail from selected mails with criterias.
                    temp = [ file for file in self.gmail_service.get_attachment(filter_mail['id'], filename_regex=filename_regex, store_dir=single_process.store_dir) ]
                    self.processed_columns_serialize(single_process.type, temp, index)
        updateRangeIndex = self.handleframe.columns.get_loc(self.configuration['const']['updateRange'])
        self.handleframe = self.handleframe.drop(self.handleframe.columns[:updateRangeIndex], axis=1)
        self.handleframe = self.handleframe.fillna('')
        updatelistBody = self.handleframe.values.tolist()
        updatelistBody.insert(0, self.handleframe.columns.tolist())
        updateBody = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    "range": '%s!%s1'%(self.configuration['const']['sheetName'], chr(65+updateRangeIndex)),
                    "majorDimension": "ROWS",
                    "values": updatelistBody
                }
            ]
        }
        response = self.gsheet_service.update_Spreadsheet(self.configuration['const']['sheetId'], updateBody)
        self.gsheet_service.update_Spreadsheet_format(self.configuration['const']['sheetId'], {'requests': self.configuration['const']['updateProperties']})


    def processed_columns_serialize(self, process_type, results, index) :
        '''
        Usage - 
        @cell values fill in and determine there's new column need to be added.

        Args -
        @process_type : process type to define which column need to be fill in.
        @results : list of fileX processed results.
        @index : current itering row index.

        Return - 
        @No Return
        '''
        current_columns = self.handleframe.columns
        offerId = self.handleframe.loc[index][self.getFields['offerId']]
        data_level = self.handleframe.loc[index][self.getFields['data_level']]
        sub_used = self.handleframe.loc[index][self.getFields['sub_used']]
        for result in results :
            error_message = 'error.'
            file_timestamps = int(result['internalDate'][:-3])
            if process_type == 'reply'  :  #using process result validate period as column name to seize value in.
                file_result = fileX.Vursor(offerId, result['file']).process(Data_Level=data_level if data_level != '' else None, Sub_Used=sub_used if sub_used != '' else None)
                try :
                    columnDateString = parse(file_result['ProcessDetail']['Validate_Period'], fuzzy_with_tokens=True)[0].strftime('%Y-%m')
                except :
                    columnDateString = error_message
            elif process_type == 'send' : #using internalDate - 30days as column name to seize value in.
                columnDateString = (datetime.fromtimestamp(file_timestamps) - timedelta(days=30)).strftime('%Y-%m')  
            if ('%s-invoice'%columnDateString not in current_columns) and (columnDateString != error_message) :
                for index, column_name in enumerate(['send', 'reply', 'invoice']) :
                    current_columns = current_columns.insert(current_columns.get_loc(self.configuration['addfrom']) + index + 1, columnDateString + '-' + column_name)
                self.handleframe = self.handleframe.reindex(columns=current_columns)
            addDict = { '%s-%s'%(columnDateString, process_type) : datetime.fromtimestamp(file_timestamps).strftime('%Y-%m-%d %H:%M:%S')} if columnDateString != error_message else {}
            if process_type == 'reply' :
                addDict.update({
                    self.updateFields['processed_log'] : result['file'] + '\n' + self.sub_strOuput(file_result) if file_result['Result'] == 'Success' else error_message,
                    self.updateFields['threshold'] : 'ready' if file_result['Result'] == 'Success' else error_message,
                    self.updateFields['thresholdId'] : result['messageId'] if file_result['Result'] == 'Success' else error_message,
                    self.getFields['processed_flag'] : datetime.fromtimestamp(file_timestamps).strftime('%Y-%m-%d %H:%M:%S')
                })
                if file_result['Result'] == 'Success' : 
                    s3name = self.s3_filestream(result['file'])
                    addDict.update({
                        self.updateFields['filename'] : s3name
                    })
                os.remove(result['file'])
            for columns, value in addDict.items() :
                self.handleframe.loc[self.handleframe[self.getFields['offerId']] == offerId, columns] = str(value)

    def sub_strOuput(self, result) : 
        return '%s = %s + %s (%s) \n out of %s'%(
            result['CommissionTotal_estimated'],
            result['CommissionDetail']['Commission_estimated'],
            result['CommissionDetail']['Reinjection_Commission_estimated'],
            ', '.join(['%s : %s'%(k, v) for k, v in result['ProcessDetail']['ValidatedDetail'].items()]),
            result['ProcessDetail']['ProcessCount']
        )
    def s3_filestream(self, path) :

        file_name, file_extension = os.path.splitext(os.path.basename(path))
        if file_extension == '.csv' :
            ContentType = 'text/csv'
        elif file_extension == '.xls' :
            ContentType = 'application/vnd.ms-excel'
        elif file_extension == '.xlsx' :
            ContentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else : 
            print('unsupport MIME format :%s'%file_extension)
        upload = self.s3.Bucket('shopback-validation').put_object(Key='%s%s'%(file_name, file_extension), Body=open(path, 'rb'), ACL='public-read', ContentType=ContentType)
        
        return '%s%s'%(file_name, file_extension)

if __name__ == '__main__' :
    try :                        
        with open('./config/validation_main_configuration.json') as f :
            configuration = json.loads(f.read())
        running_instance = instance(configuration)
    except :
        raise ValueError('''config error that instance could not be initialize''')

    for process in running_instance.configuration['process_iteration'] :
        running_instance = instance(configuration)
        p = single_process(process)
        print('start processing %s process'%p.type)
        running_instance.process_container(p)
        print('finish processing %s process'%p.type)
