import sys, csv, collections, pymysql, json, requests, threading, datetime, time, math, os
import numpy as np
import pandas as pd
from dateutil.parser import parse

database = pymysql.connect(host='*.*.*.*', port=3306, user='**', password='****', database='ShopBack', charset='utf8', autocommit='true')
threads = []

class Vursor() :
    
    def __init__(self, offer_id, addr) :
        self.offer_id = offer_id
        self.addr = addr
        self.header = None
        self.status = None
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''SELECT Header_Ref, Status_Ref FROM Merchant_Data WHERE Offer_id = "%s"'''%self.offer_id)
        data = cursor.fetchone()
        if data :
            if data['Header_Ref'] and data['Status_Ref'] :
                self.header = data['Header_Ref'].decode('utf-8')
                self.status = data['Status_Ref'].decode('utf-8')

    def snippet(self, header=None) :
        if header :
            self.header = header
        return fileProcessing('Snippet', self.offer_id, self.addr, self.header)

    def process(self, header=None, status=None, Sub_Used=None, Payout_Remain=None, Pending_Handle=None, Data_Level=None) : 
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''SELECT Header_Ref, Status_Ref FROM Merchant_Data WHERE offer_id = "%s"'''%self.offer_id)
        data = cursor.fetchone()
        if header and status :
            self.header = header
            self.status = status
        try : 
            self.temp_file = fileProcessing('Process', self.offer_id, self.addr, self.header, self.status, Sub_Used, Payout_Remain, Pending_Handle, Data_Level)
            return self.temp_file['information']
        except :
            self.temp_file = {
                'Method' : 'Process',
                'information' : {
                    'OfferId' : self.offer_id,
                    'Result' : 'Failed',
                    'CommissionTotal_estimated' : False,
                    'CommissionDetail' : {
                        'Commission_estimated' : False,
                        'Reinjection_Commission_estimated' : False,
                    },
                    'ProcessDetail' : {
                        'ProcessCount' : 0,
                        'ValidatedDetail' : {'approved' : 0, 'rejected' : 0},
                        'Unprocessed' : 0,
                        'Reinjection' : 0,
                        'Validate_Period' : False,
                        'UploadCounts' : {
                            'Status' : 0,
                            'Amount' : 0,
                            'Payout' : 0
                        }
                    }
                }
            }
            return self.temp_file['information']
        
    def write(self, path) :
        if self.temp_file == None :
            print ('No File has been processed yet.')
            return False
        if not os.path.exists(path):
            os.makedirs(path)
        return conversionOverride(self.temp_file, self.offer_id, path)
    
    def call(self) :
        if self.temp_file == None :
            print ('No File has been processed yet.')
            return False
        threads.append(threading.Thread(target=APIconversionOverride, args = (self.temp_file, self.offer_id)))
        threads[len(threads)-1].start()
        
    def save_config(self) :
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''UPDATE Merchant_Data SET Header_Ref = '%s', Status_Ref = '%s' WHERE Offer_ID = "%s"'''%(self.header, self.status, self.offer_id))
        cursor.execute('''COMMIT''')

    

def fileProcessing(action, offer_id, workfile, header=None, status=None, Sub_Used=None, Payout_Remain=None, Pending_Handle="pending", Data_Level=None) :

    df = pd.read_excel(workfile, None, na_values=['NA'])
    sheet_names = df.keys()
    sheet_index = dict()
    mega_file = dict()
    time_ls = []
    for i, name in enumerate(sheet_names) :
        try : 
            sheet_range = json.loads(header)
            if str(i) not in sheet_range.keys():
                continue
        except (TypeError, AttributeError) :
            print("this offer %s haven't setup status config yet."%offer_id)
        df_sub = df[name]
        status_set = set()
        headers = df_sub.columns
        header_index = 0
        for j in range(15) :
        # If can't find valid header in 15 step, this sheet can regard as broken.
            headers = rebuild_Name(headers)
            # Fix header into standard format.
            if header_validCheck(headers) :
                break
            else :
                headers = df_sub.iloc[header_index]
                header_index += 1
        ndf = pd.read_excel(workfile, None, na_values=['NA'], header = header_index)
        df_sub = ndf[name]

        if header :
            header_used = json.loads(header)
            index_Status = int(header_used[str(i)]['Status_index'])
            index_Datetime = int(header_used[str(i)]['Datetime_index'])
            for row in df_sub.itertuples():
                try :
                    if pd.isnull(row[index_Status + 1]) :
                        status_set.add('null')
                    else :
                        status_set.add(row[index_Status + 1])
                    try :
                        datetimeObj = parse(str(row[index_Datetime+1]), fuzzy_with_tokens=True)
                    except (TypeError, ValueError) :
                        continue
                    time_ls.append(datetimeObj[0])
                except IndexError :
                    continue  
        else :
            status_set.add('''Offer : %s hasn't define its Status_index.'''%offer_id)
       
        sample_row_raw = rebuild_Name(df_sub.iloc[header_index+1])
        sample_row = list()

        for each in sample_row_raw :
            sample_row.append(str(each))

        sheet_index[i] = {
            'header' : list(headers),
            'status' : list(status_set),
            'sample' : list(sample_row),
            'sheet_name' : name,
        }
        mega_file[i] = {
            'header' : headers,
            'dataframe' : df_sub
        }

    if action == 'Snippet' :
        sheet_index['validate_period'] = max(time_ls)
        return sheet_index
        # Instant log sample for client site check and put value in.

    elif action == 'Process' : 
        
        if not header and not status :
            raise ValueError('''Haven't set up either header or status in database yet.''')
        else :
            header = json.loads(header)
            status = json.loads(status)

        merchant_dict = dict()
        time_unixls = list()

        for page_index in mega_file :          
            df_sub = mega_file[page_index]['dataframe']
            selected_cols = [
                df_sub.columns[int(header[str(page_index)]['OrderID_index'].split('_')[0])],
                df_sub.columns[int(header[str(page_index)]['Amount_index'])],
                df_sub.columns[int(header[str(page_index)]['Payout_index'])],
                df_sub.columns[int(header[str(page_index)]['Datetime_index'])],
                df_sub.columns[int(header[str(page_index)]['Status_index'])],
                df_sub.columns[int(header[str(page_index)]['Note_index'])]
                ]
                
            if len(header[str(page_index)]['OrderID_index'].split('_')) > 1 :
            # Uniqlize the order id of Merchant report.
                for sub_col in header[str(page_index)]['OrderID_index'].split('_')[1:] :
                    selected_cols.append(df_sub.columns[int(sub_col)])
            merchant_df = df_sub[selected_cols]
            # Get maximun date of this validation period.
            time_df = df_sub[df_sub.columns[int(header[str(page_index)]['Datetime_index'])]].tolist()
            ready_drop = list()
            for row, time in enumerate(time_df) :        
                try :
                    datetimeObj = parse(str(time), fuzzy_with_tokens=True)
                    time_unixls.append(int(datetimeObj[0].strftime('%s')))
                except (TypeError, ValueError) :
                    ready_drop.append(row)
                    # If the time column can't be parsed into time object, abandon this row.
                    continue
            merchant_df = merchant_df.drop(merchant_df.index[ready_drop])
            merchant_page_dict = collections.defaultdict(list)
            for conversion in merchant_df.itertuples() :
                try :
                    merchant_orderid = str(int(float(conversion[1]))) # Remove float after order ID.
                except ValueError :
                    merchant_orderid = str(conversion[1])
                merchant_amount = conversion[2]
                merchant_payout = conversion[3]
                merchant_time = parse(str(conversion[4]), fuzzy_with_tokens=True)[0].strftime('%Y/%m/%d %H:%M:%S')
                merchant_status = conversion[5]
                merchant_note = conversion[6]
                if not pd.isnull(merchant_status) :
                    merchant_status = str(conversion[5]).lower()
                else :
                    merchant_status = 'null'
                info = {
                        'order_id' : merchant_orderid,
                        'datetime' : merchant_time,
                        'amount' : merchant_amount,
                        'payout' : merchant_payout,
                        'status' : merchant_status,
                        'note' : merchant_note
                    }
                flag = 7
                while True :
                # Get all additional value add into order_id and seperated with '_'.
                    try : 
                        try :
                            info['order_id'] += '_' + str(int(conversion[flag]))
                        except ValueError :
                            info['order_id'] += '_' + str(conversion[flag])
                        flag += 1
                    except IndexError :
                        break
                merchant_page_dict[info['order_id']].append(info)              
            merchant_dict[page_index] = merchant_page_dict
        # Get conversion by calling API.
        page = 1
        edate = datetime.datetime.fromtimestamp(max(time_unixls)).strftime('%Y-%m-%d')
        API_url = 'https://shopback.api.hasoffers.com/Apiv3/json?NetworkToken=***&Target=Report&Method=getConversions&fields[]=Stat.payout&fields[]=Stat.datetime&fields[]=Stat.sale_amount&fields[]=Stat.currency&fields[]=ConversionsMobile.adv_sub5&fields[]=ConversionsMobile.adv_sub4&fields[]=ConversionsMobile.adv_sub3&fields[]=ConversionsMobile.adv_sub2&fields[]=Stat.advertiser_info&fields[]=Stat.id&filters[Stat.status][conditional]=EQUAL_TO&filters[Stat.status][values]=pending&filters[Stat.datetime][conditional]=LESS_THAN_OR_EQUAL_TO&filters[Stat.datetime][values]={}+23%3A59%3A59+&filters[Stat.offer_id][conditional]=EQUAL_TO&filters[Stat.offer_id][values]={}&page={}&totals=1'
        result = requests.get(API_url.format(edate, offer_id, page)).text
        result = json.loads(result)
        sb_dict = collections.defaultdict(list)
        map_result = collections.defaultdict(list)
        if result['response']['data']['count'] is not None :
            data = result['response']['data']
            pageCount = data['pageCount']
            for page in range(1,pageCount+1) :
                result = requests.get(API_url.format(edate, offer_id, page)).text
                result = json.loads(result)
                conversions = result['response']['data']['data']
                for conversion in conversions : 
                    try :
                        sb_orderid = str(int(conversion['Stat']['advertiser_info']))
                    except ValueError :
                        sb_orderid = str(conversion['Stat']['advertiser_info'])
                    sb_id = conversion['Stat']['id']
                    sb_datetime = conversion['Stat']['datetime']
                    sb_currency = conversion['Stat']['currency']
                    sb_payout = conversion['Stat']['payout@' + sb_currency]
                    sb_amount = conversion['Stat']['sale_amount@' + sb_currency]
                    if Sub_Used : 
                        for sub in Sub_Used.split('_') : 
                            if int(sub) > 5 :
                                raise ValueError('sub index must be under 5.')
                            if int(sub) == 1 :
                                continue
                            try :
                                sb_orderid += '_' + str(int(conversion['ConversionsMobile']['adv_sub' + sub ]))
                            except ValueError :
                                sb_orderid += '_' + str(conversion['ConversionsMobile']['adv_sub' + sub ])
                        if Sub_Used.split('_')[0] != '1' :
                            sb_orderid = sb_orderid.split('_')
                            sb_orderid = '_'.join(sb_orderid[1:])
                    # Uniqlize the order id on sbho.
                    sb_dict[sb_orderid].append(
                        {
                            'id' : sb_id,
                            'order_id' : sb_orderid,
                            'datetime' : sb_datetime,
                            'amount' : int(float(sb_amount)),
                            'payout' : float(sb_payout)
                        }
                    )
                    map_result[sb_id] = {
                        'amount' : int(float(sb_amount)),
                        'payout' : float(sb_payout)
                    }
        else :
            print('''No Pending orders need to be validated on Hasoffers for %s.'''%offer_id)
        data = {
            'sb_dict' : sb_dict,
            'merchant_dict' : merchant_dict,
            'status' : status,
            'map_result' : map_result,
            'Offer_ID' : offer_id,
            'Pending_Handle' : Pending_Handle,
            'Data_Level' : Data_Level,
            'edate' : edate,
            'Payout_Remain' : Payout_Remain
        }

        result = conversionUpdate(data)
        return result
        

# Start processing data mapping.
def conversionUpdate(data) :
    
    # !-------NOTE-------!
    # Format of data validation below : ['key':[{},{},{}]] <-- in Merchant File, dict behind will be added up // in SBHO, dict behind will also be added up if its not `order-level`

    # *** - Duplicated Order Handle Rules :
        # 1. If the orders were tracked in `order-level` and go duplicated, will rejected every data on sbho except first one.
        # 2. If the orders were tracked in `item-level` and key is not unique, use KnapSack to find the order which is canceled and caused into sale_amount gap.

    # *** - Nonetype value in file :
        # 1. Either payout or sale_amount does not given in the report, we use value on SBHO (first one if there's dupes) to substitute.
        # 2. Nonetype rejected order in file will use 0 to substitute.

    # *** - Update amount or payout :
        # 1. Since data has been cleaned by above handle, SBHO order will always use Merchant file sale_amount and payout as redeemable value.
    sb_dict = data['sb_dict']
    merchant_dict = data['merchant_dict']
    status = data['status']
    map_result = data['map_result']
    Data_Level = data['Data_Level']
    Pending_Handle = data['Pending_Handle']
    Offer_ID = data['Offer_ID']
    meta_upload = list()
    reinjection_upload = list()
    pending_upload = list()
    meta_pendings = dict()
    meta_reinjections = dict()
    for page_index in merchant_dict : # Iter sheet.
        mc_dict = merchant_dict[page_index]
        intersections = set(sb_dict.keys()) & set(mc_dict.keys())
        reinjection = set(mc_dict.keys()) - set(sb_dict.keys())
        pendings = set(sb_dict.keys()) - set(mc_dict.keys())
        meta_reinjections[page_index] = reinjection
        meta_pendings[page_index] = pendings
        page_status = status[str(page_index)]
        page_status['approved'] = 'approved'
        page_status['rejected'] = 'rejected'
        for order_id in intersections :
            sb_orders = sb_dict[order_id]
            mc_orders = mc_dict[order_id]
            mc_orders = TypeAdjust(mc_orders, sb_orders, page_status, Data_Level) # Adjust null value.
            if len(mc_orders) > 1 :
                mc_orders = DupesSerialize('Merchant', mc_orders, order_id, page_status)
            mc_order = mc_orders[0]
            redeem_status = page_status[mc_order['status']]
            if redeem_status == 'rejected' :
            # if mc_order(Serialized) status is rejected, rejected all on shopback.
                for sb_order in sb_orders :
                    api_params_dict = get_paramsTemplate()
                    api_params_dict['id'] = sb_order['id']
                    api_params_dict['order_id'] = order_id.split('_')[0]
                    api_params_dict['datetime'] = sb_order['datetime']
                    api_params_dict['payout'] = sb_order['payout']
                    api_params_dict['revenue'] = sb_order['payout']
                    api_params_dict['sale_amount'] = sb_order['amount']
                    api_params_dict['status'] = redeem_status
                    api_params_dict['note'] = mc_order['note']
                    meta_upload.append(api_params_dict)
            elif redeem_status == 'approved' :
                if len(sb_orders) > 1 and Data_Level :
                # Item Level.   
                    comparing_orders = DupesSerialize('ShopBack', sb_orders, order_id)
                    comparing_order = comparing_orders[0]
                    if comparing_order['amount'] <= mc_order['amount'] or comparing_order['payout'] <= mc_order['payout'] :
                    # If this order on Shopback goes dupes, but amount and payout remain the same.
                        for sb_order in sb_orders :
                            api_params_dict = get_paramsTemplate()
                            api_params_dict['id'] = sb_order['id']
                            api_params_dict['order_id'] = order_id.split('_')[0]
                            api_params_dict['datetime'] = sb_order['datetime']
                            api_params_dict['payout'] = sb_order['payout']
                            api_params_dict['revenue'] = sb_order['payout']
                            api_params_dict['sale_amount'] = sb_order['amount']
                            api_params_dict['status'] = redeem_status
                            api_params_dict['note'] = mc_order['note']
                            meta_upload.append(api_params_dict)
                    else :
                        subset = Knapsack(sb_orders, mc_order['amount'])
                        for sb_order in sb_orders :
                            api_params_dict = get_paramsTemplate()
                            api_params_dict['id'] = sb_order['id']
                            api_params_dict['order_id'] = order_id.split('_')[0]
                            api_params_dict['datetime'] = sb_order['datetime']
                            api_params_dict['payout'] = sb_order['payout']
                            api_params_dict['revenue'] = sb_order['payout']
                            api_params_dict['sale_amount'] = sb_order['amount']
                            if len(subset) != 0 :
                                if api_params_dict['sale_amount'] in subset :
                                    api_params_dict['status'] = redeem_status
                                    subset[subset.index(api_params_dict['sale_amount'])] = str(api_params_dict['sale_amount'])
                                else :
                                    api_params_dict['status'] = 'rejected'
                                    api_params_dict['note'] = '''Partial refund $%s of order: %s'''%(sb_order['amount'], order_id.split('_')[0])
                            else :
                                print('''Can't find subset can only approve all''')
                                api_params_dict['status'] = redeem_status
                                api_params_dict['note'] = '''Can't find subset of $%s amount'''%(sb_order['amount'])
                            meta_upload.append(api_params_dict)           
                else :
                # Order Level.
                    sb_order = sb_orders[0]
                    api_params_dict = get_paramsTemplate()
                    api_params_dict['id'] = sb_order['id']
                    api_params_dict['order_id'] = order_id.split('_')[0]
                    api_params_dict['datetime'] = sb_order['datetime']
                    api_params_dict['sale_amount'] = mc_order['amount']
                    if mc_order['amount'] == 0 :
                        api_params_dict['payout'] = 0
                    else : # Use Merchant amount and use our own rate if the rate's difference is neglatable.
                        if mc_order['amount'] == sb_order['amount'] :
                            rate = sb_order['payout']/sb_order['amount'] if abs(sb_order['payout']/sb_order['amount'] - mc_order['payout']/mc_order['amount']) < 0.001 else round(mc_order['payout']/mc_order['amount'],3)
                        elif mc_order['amount'] != sb_order['amount'] :
                            rate = round(mc_order['payout']/mc_order['amount'],3)
                        api_params_dict['payout'] = mc_order['amount'] * rate
                        api_params_dict['revenue'] = mc_order['amount'] * rate
                    api_params_dict['status'] = redeem_status
                    api_params_dict['note'] = mc_order['note']
                    meta_upload.append(api_params_dict)
                    for rest in sb_orders[1:] :
                        # If offer tracked in order level but Hasoffer goes duplicated, rejected.
                        api_params_dict = get_paramsTemplate()
                        api_params_dict['id'] = rest['id']
                        api_params_dict['order_id'] = order_id.split('_')[0]
                        api_params_dict['datetime'] = sb_order['datetime']
                        api_params_dict['payout'] = rest['payout']
                        api_params_dict['revenue'] = rest['payout']
                        api_params_dict['sale_amount'] = rest['amount']
                        api_params_dict['status'] = 'rejected'
                        api_params_dict['note'] = '''Rejected because of invalid duplication in order level tracking.''' 
                        meta_upload.append(api_params_dict)
            else :
                continue

    meta_pendings = SetExtraction(meta_pendings)
    for pending_orderid in meta_pendings :
        pending_orders = sb_dict[pending_orderid]
        for pending_order in pending_orders :
            api_params_dict = get_paramsTemplate()
            api_params_dict['id'] = pending_order['id']
            api_params_dict['payout'] = pending_order['payout']
            api_params_dict['revenue'] = pending_order['payout']
            api_params_dict['sale_amount'] = pending_order['amount']
            api_params_dict['status'] = Pending_Handle
            pending_upload.append(api_params_dict)
    reinjection_commission = 0
    for page in meta_reinjections :
        for reinjection_orderid in meta_reinjections[page] :
            reinjection_orders = merchant_dict[page][reinjection_orderid]
            for reinjection_order in reinjection_orders :
                reinjection_dict = {
                    'affiliate_id' : 1059,
                    'offer_id' : Offer_ID,
                    'sale_amount' : reinjection_order['amount'],
                    'payout' : reinjection_order['payout'],
                    'revenue' : reinjection_order['payout'],
                    'datetime' : reinjection_order['datetime'],
                    'advertiser_info' : reinjection_orderid,
                    'status' : 'approved'
                }
                if status[str(page)][reinjection_order['status']] != 'approved' :
                    continue
                if pd.isnull(reinjection_dict['payout']):
                    reinjection_dict['payout'] = reinjection_dict['sale_amount'] * (meta_upload[0]['payout']/meta_upload[0]['sale_amount'])
                reinjection_commission += reinjection_dict['payout']
                reinjection_upload.append(reinjection_dict)
    if pd.isnull(reinjection_commission) :
        reinjection_commission = 0

    master_df = pd.DataFrame(meta_upload, columns=['id', 'order_id', 'datetime', 'sale_amount', 'payout', 'revenue', 'status', 'note'])
    master_df.loc[master_df.payout == 0, 'status'] = 'rejected'
    status_df = pd.DataFrame(columns=['id','status'])
    payout_df = pd.DataFrame(columns=['id','payout','revenue'])
    amount_df = pd.DataFrame(columns=['id','sale_amount'])
    reinjection_df = pd.DataFrame(reinjection_upload, columns=['affiliate_id','offer_id', 'sale_amount', 'payout', 'revenue', 'datetime', 'advertiser_info','status'])
    reinjection_df_layout = reinjection_df.drop(columns=['affiliate_id', 'offer_id'])
    reinjection_df_layout = reinjection_df_layout.rename(columns={'advertiser_info':'order_id'})
    layout_df = master_df.append(reinjection_df_layout, sort=False)

    for index, master_row in master_df.iterrows() :
        stat_id = master_row['id']
        payout = master_row['payout']
        amount = master_row['sale_amount']
        status = master_row['status']
        if status == 'approved' :
            if abs(round(payout) - round(map_result[stat_id]['payout'])) > 1 :
                payout_df.loc[len(payout_df)] = [stat_id, payout, payout]
            if abs(amount - map_result[stat_id]['amount']) > 1 :
                amount_df.loc[len(amount_df)] = [stat_id, amount]
        elif status == 'pending' :
            continue   
        status_df.loc[len(status_df)] = [stat_id, status]
    if data['Payout_Remain'] :
        payout_df = payout_df[['id','revenue']]
        
    return {
        'Method' : 'Process',
        'information' : {
            'OfferId' : Offer_ID,
            'Result' : 'Success' if len(meta_upload) != 0 else 'Failed',
            'CommissionTotal_estimated' : round(master_df.loc[master_df['status']=='approved', 'revenue'].sum()) + round(reinjection_commission),
            'CommissionDetail' : {
                'Commission_estimated' : round(master_df.loc[master_df['status']=='approved', 'revenue'].sum()),
                'Reinjection_Commission_estimated' : round(reinjection_commission),
            },
            'ProcessDetail' : {
                'ProcessCount' : len(meta_upload),
                'ValidatedDetail' : dict(status_df['status'].value_counts()),
                'Unprocessed' : len(meta_pendings),
                'Reinjection' : len(reinjection_upload),
                'Validate_Period' : data['edate'],
                'UploadCounts' : {
                    'Status' : len(status_df),
                    'Amount' : len(amount_df),
                    'Payout' : len(payout_df)
                }
            }
        },
        'sheets' : {
            'Status.csv' : status_df,
            'Amount.csv' : amount_df,
            'Payout.csv' : payout_df,
            'Reinject.csv' : reinjection_df,
            'Report.csv' : layout_df
        }
    }
def APIconversionOverride(sheet_dic, offer_id) :
    
    import time
    api_url = 'https://shopback.api.hasoffers.com/Apiv3/json'
    report_data = sheet_dic['sheets']['Report.csv']
    status_data = sheet_dic['sheets']['Status.csv']
    amount_data = sheet_dic['sheets']['Amount.csv']
    payout_data = sheet_dic['sheets']['Payout.csv']
    queue = []
    for row_index in range(len(status_data)) :
        params = get_params()
        params['id'] = status_data.iloc[row_index]['id']
        params['data[status]'] = status_data.iloc[row_index]['status']
        sr = requests.get(api_url, params=params)
        errMsg = json.loads(sr.text)['response']['errorMessage']
        if errMsg is not None :
            print('%s / occured on calling status call of %s'%(errMsg, params['id']))
            queue.append(params)
            continue
        print('success make call on id %s of offer_id %s as status %s'%(params['id'], offer_id, params['data[status]']))
        time.sleep(0.1)
        if params['data[status]'] == 'rejected' :
            note_params = get_params()
            note_params['note'] = params['id']
            note_params['Method'] = 'updateMeta'
            status_note = report_data.loc[report_data['id'] == params['id']].note.values[0]
            note_params['data[note]'] = status_note
            nr = requests.get(api_url, params=note_params)
            errMsg = json.loads(nr.text)['response']['errorMessage']
            if errMsg is not None :
                print('%s / occured on calling note call of %s'%(errMsg, params['id']))
                queue.append(note_params)
                continue

        if params['id'] in amount_data.id.unique() :
            # amount update call if this id exist in amount df.
            amount_params = get_params()
            amount_params['id'] =  params['id']
            amount_params['data[sale_amount]'] = round(amount_data.loc[amount_data['id'] == params['id']].sale_amount.values[0],2)
            ar = requests.get(api_url, params=amount_params)
            errMsg = json.loads(ar.text)['response']['errorMessage']
            if errMsg is not None :
                print('%s / occured on calling note call of %s'%(errMsg, params['id']))
                queue.append(amount_params)
                continue
            print('success make call on id %s of offer_id %s as amount changed %s'%(params['id'], offer_id, amount_params['data[sale_amount]']))
            time.sleep(0.2)

        if params['id'] in payout_data.id.unique() :
            # payout and revenue update call if this id exist in payout df.
            payout_params = get_params()
            payout_params['id'] =  params['id']
            payout_params['data[payout]'] = round(payout_data.loc[payout_data['id'] == params['id']].payout.values[0],2)
            payout_params['data[revenue]'] = round(payout_data.loc[payout_data['id'] == params['id']].payout.values[0],2)
            pr = requests.get(api_url, params=payout_params)
            errMsg = json.loads(pr.text)['response']['errorMessage']
            if errMsg is not None :
                print('%s / occured on calling note call of %s'%(errMsg, params['id']))
                queue.append(payout_params)
                continue
            print('success make call on id %s of offer_id %s as payout and revenue changed %s'%(params['id'], offer_id, payout_params['data[payout]']))
            time.sleep(0.2)
    
    for q in queue :
        qr = requests.get(api_url, params=q)
        time.sleep(0.1)
    

    

def conversionOverride(sheet_dic, offer_id, path='./') :
    
    r = requests.get('https://shopback.api.hasoffers.com/Apiv3/json?NetworkToken=***&Target=Offer&Method=findAllByIds&ids[]=%s'%offer_id)
    name = json.loads(r.text)['response']['data'][str(offer_id)]['Offer']['name']
    month = sheet_dic['information']['ProcessDetail']['Validate_Period'].split('-')[1]
    sheet_dic = sheet_dic['sheets']

    for sheet in sheet_dic :
        if len(sheet_dic[sheet]) == 0 :
            continue
        elif len(sheet_dic[sheet]) > 10000 and sheet not in ['Reinject.csv', 'Report.csv']:
            tag = 0
            for i in range(math.ceil(len(sheet_dic[sheet])/10000)) :
                sheet_dic[sheet][tag : (i + 1) * 10000].to_csv( path +'%s_%s_%s_'%(name, month, i) + sheet, encoding='utf-8-sig', index=False)
                tag = (i + 1) * 10000
        else :
            sheet_dic[sheet].to_csv( path +'%s_%s_'%(name, month) + sheet, encoding='utf-8-sig', index=False)
    return {
        'report' : path + '%s_%s_'%(name, month) + 'Report.csv'
    }
    

def get_paramsTemplate() :
    
    return {
        'id' : None,
        'order_id' : None,
        'datetime' : None,
        'payout' : None,
        'revenue' : None,
        'sale_amount' : None,
        'status' : None,
        'note' : None
    }
def get_params() :
    
    params = {
        'NetworkToken' : '***',
        'Target' : 'Conversion',
        'Method' : 'update'
    }
    return params
                
def header_validCheck(headers) :
    
    flag = 0
    for col in headers :
        if pd.isnull(col) or 'Unnamed' in str(col):
            flag += 1
    if flag/len(headers) < 0.25 :
    # Quality check for each row has contained over 25% nan or Unnamed. 
        return True 
    else :
        return False

def TypeAdjust(mc_orders, sb_orders, page_status, Data_Level) :
    
    # if its none type, whether it's order or item level - we keep the value as same as SBHO, then there's won't be an further update.
    for mc_order in mc_orders :
        status = page_status[mc_order['status']]
        if pd.isnull(mc_order['amount']) or type(mc_order['amount']) not in (int, float):
            if status == 'rejected' :
                mc_order['amount'] = 0
                mc_order['payout'] = 0                  
            else :
                if len(sb_orders) > 1 and Data_Level:
                    raise ValueError("Can't handle null amount with duplicated orders.")
                else :
                    sb_order = sb_orders[0]
                    mc_order['amount'] = sb_order['amount']
                    mc_order['payout'] = sb_order['payout']
        if pd.isnull(mc_order['payout']) or type(mc_order['payout']) not in (int, float):
            if status == 'rejected' :
                mc_order['payout'] = 0
            else :
                if len(sb_orders) > 1 and Data_Level:
                    raise ValueError("Can't handle null payout with duplicated orders.")
                else :
                    sb_order = sb_orders[0]
                    mc_order['payout'] = sb_order['payout']
    return mc_orders          

def SetExtraction (page_set) :
    
    flag_set = set()
    for page in page_set :
        current_set = page_set[page]
        flag_set = flag_set.symmetric_difference(current_set)
    return flag_set
    
        
def rebuild_Name(values) :
    
    if type(values.name) in [ int, np.int64 ] or values.name is None :
        sample_row = values.tolist()
    else :
        sample_row = list(values.name) + values.tolist()

    return sample_row

def DupesSerialize(action, orders, order_id, status=None) :
    
    if action == 'Merchant' :

        rebuild = [{
            'order_id' : order_id,
            'datetime' : orders[0]['datetime'],
            'amount' : 0,
            'payout' : 0,
            'status' : 'rejected',
            'note' : None
        }]
        for order in orders :
            if status[order['status']] == 'approved' :
                rebuild[0]['status'] = 'approved'
            elif status[order['status']] == 'rejected' : 
                rebuild[0]['note'] = 'partial refund'
                if order['payout'] > 0 and order['amount'] > 0 :
                # Rejected info should be summed as negative.
                    order['payout'] ,order['amount'] = -1 * abs(order['payout']),  -1 * abs(order['amount'])
            rebuild[0]['amount'] += order['amount']
            rebuild[0]['payout'] += order['payout']
        return rebuild

    elif action == 'ShopBack' : 
        
        this_payout_sum = 0
        this_amount_sum = 0
        orders_list = list ()
        for order in orders :
            this_payout_sum += order['payout']
            this_amount_sum += order['amount']
            edited = get_paramsTemplate()
            edited['id'] = order['id']
            edited['order_id'] = order['order_id']
            edited['status'] = 'approved'
            edited['payout'], edited['amount'] = 0, 0
            edited['datetime'] = order['datetime']
            orders_list.append(edited)

        orders_list[0]['amount'], orders_list[0]['payout'] = this_amount_sum, this_payout_sum
        return orders_list
            
def Knapsack(amounts, sum) :
    def f(v, i, S, memo):
        if i >= len(v): return 1 if S == 0 else 0
        if (i, S) not in memo:  # <-- Check if value has not been calculated.
            count = f(v, i + 1, S, memo)
            count += f(v, i + 1, S - v[i], memo)
            memo[(i, S)] = count  # <-- Memoize calculated result.
        return memo[(i, S)]     # <-- Return memoized value.
    def g(v, S, memo):
        subset = []
        for i, x in enumerate(v):
            # Check if there is still a solution if we include v[i]
            if f(v, i + 1, S - x, memo) > 0:
                subset.append(x)
                S -= x
        return subset
    memo = dict()
    v = list()
    for amount in amounts :
        v.append(amount['amount'])
    if f(v, 0, sum, memo) == 0: 
        print(amount, sum)
        raise ValueError("There's no subset qualified.")
    else: 
        return g(v, sum, memo)
        

    
    
    
            

    
    
    
