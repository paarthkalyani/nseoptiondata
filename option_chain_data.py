import requests
import json
import os
import pandas as pd
import datetime
import time


url_oc      = "https://www.nseindia.com/option-chain"
url_bnf     = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
url     = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
url_indices = "https://www.nseindia.com/api/allIndices"

# Headers
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}






def option_chain_data_collecter():
    sess = requests.Session()
    cookies = dict()

    # Local methods
    def set_cookie():
        request = sess.get(url_oc, headers=headers, timeout=5)
        cookies = dict(request.cookies)

    def get_data(url):
        set_cookie()
        response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
        if(response.status_code==401):
            set_cookie()
            response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
            #print(response)
        if(response.status_code==200):
            return response.text
        return ""





    response_text = get_data(url)
    data = json.loads(response_text)
    exp_list = []


    for item in data["records"]["expiryDates"]:
            # Directory 
        #path = ".\\expiry_data\\{}".format(item)
        directory = item    
        parent_dir = ".\\expiry_data\\"
        path = os.path.join(parent_dir, directory) 
        if not os.path.exists(path):
            os.makedirs(path) 

        exp_list.append(item)


    for item in data["records"]["data"]:
        directory = item["expiryDate"]
        for key in item.keys():
            if key == "PE":
                option_type = "PE"
            else:
                option_type = "CE"
        file_name = str(item["strikePrice"]) + option_type + ".csv"
        parent_dir = ".\\expiry_data\\"
        path = os.path.join(parent_dir, directory)
        final_path = os.path.join(path, file_name)
        columns = ["Strike_Price", "Expiry_Date","OI","Change_OI","%Change_OI","Volume","IV","LTP","LTP_change","%LTP_change",
            "Total_Buy_Quantity","Total_Sell_Quantity","Bid_Quantity","Bid_Price","Ask_Quantity","Ask_Price","Underlying_Price"]
        index= 1


        option_data =[
                    [item[option_type]["strikePrice"],item[option_type]["expiryDate"],item[option_type]['openInterest'],item[option_type]['changeinOpenInterest'],item[option_type]['pchangeinOpenInterest'],
                    item[option_type]['totalTradedVolume'],item[option_type]['impliedVolatility'],item[option_type]['lastPrice'],item[option_type]['change'],item[option_type]['pChange'],
                    item[option_type]['totalBuyQuantity'],item[option_type]['totalSellQuantity'],item[option_type]['bidQty'],item[option_type]['bidprice'],item[option_type]['askQty'],item[option_type]['askPrice'],
                    item[option_type]['underlyingValue']  ]
                    ]
            
        if not os.path.exists(final_path):
            with open(final_path, 'w') as fp:
                df = pd.DataFrame(option_data , columns = columns , index = [datetime.datetime.now()])

        else:
            old_df = pd.read_csv(final_path , index_col = 0)
            new_df = pd.DataFrame(option_data , columns = columns, index = [datetime.datetime.now()])
            df = pd.concat([old_df,new_df])
        df.to_csv(final_path)



    parent_dir = ".\\expiry_option_chain\\"
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)

    columns = ["Time","OI","Change_OI","%Change_OI","Volume","IV","LTP","LTP_change","%LTP_change",
            "Total_Buy_Quantity","Total_Sell_Quantity","Bid_Quantity","Bid_Price","Ask_Price","Ask_Quantity",
            "Strike_Price",
            'Ask_Quantity', 'Ask_Price', 'Bid_Price', 'Bid_Quantity', 'Total_Sell_Quantity', 'Total_Buy_Quantity', '%LTP_change', 'LTP_change', 'LTP', 'IV', 'Volume', '%Change_OI', 'Change_OI', 'OI']

    all_exp_dict = {}


    for expiry in exp_list:
        all_exp_dict[expiry] = {}                                                # creates 2nd level dictionary of each expiry, in main dictionary
        for option in data["records"]["data"]:                                   # if records[data] have same expiry, add data
            if option['expiryDate'] == expiry:
                all_exp_dict[expiry][option["strikePrice"]]={"CE":[],"PE":[]}    #add data: step 1: create 3rd level strike price and 4th level optiontype dictionary for Put & Call Options, record['data'] have only one option. Add data to this third level dictionary

    for expiry in exp_list:
        for option in data["records"]["data"]:
            if option['expiryDate'] == expiry:

                if "CE" in option.keys():
                    all_exp_dict[expiry][option["strikePrice"]]["CE"] = [option["CE"]['openInterest'],option["CE"]['changeinOpenInterest'],option["CE"]['pchangeinOpenInterest'],option["CE"]['totalTradedVolume'],option["CE"]['impliedVolatility'],option["CE"]['lastPrice'],
                    option["CE"]['change'],option["CE"]['pChange'],option["CE"]['totalBuyQuantity'],option["CE"]['totalSellQuantity'],option["CE"]['bidQty'],option["CE"]['bidprice'],option["CE"]['askPrice'],option["CE"]['askQty']]  
                if "PE" in option.keys():
                    all_exp_dict[expiry][option["strikePrice"]]["PE"] =[option["PE"]['askQty'],option["PE"]['askPrice'],option["PE"]['bidprice'],option["PE"]['bidQty'],  option["PE"]['totalSellQuantity'],option["PE"]['totalBuyQuantity'],option["PE"]['pChange'],
                option["PE"]['change'],option["PE"]['lastPrice'],option["PE"]['impliedVolatility'],option["PE"]['totalTradedVolume'],option["PE"]['pchangeinOpenInterest'],option["PE"]['changeinOpenInterest'],option["PE"]['openInterest']]

    for exp in all_exp_dict:
        expiry_data_l = []              #numpy array first bracket
        for strike in all_exp_dict[exp]:
            if len(all_exp_dict[exp][strike]["CE"]) != 0 and len(all_exp_dict[exp][strike]["PE"]) != 0:       # if both ce and pe data available
                strike_wise_list = []           #numpy array second bracket
                strike_wise_list.append(datetime.datetime.now())                       #making a list of excel columns  time- first         
                for item in all_exp_dict[exp][strike]["CE"]:                          # making a list of excel columns ce columns        
                    strike_wise_list.append(item)
                strike_wise_list.append(strike)
                for item in all_exp_dict[exp][strike]["PE"]:                        # making a list of excel columns pe columns
                    strike_wise_list.append(item)
                expiry_data_l.append(strike_wise_list)



        dir_name = str(exp)
        file_name = "{}.csv".format(datetime.datetime.now().strftime("%d-%m-%y %H.%M"))
        file_dir = os.path.join(parent_dir, dir_name )
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        path = os.path.join(file_dir,file_name)
        #if not os.path.exists(path):
        #with open(path, 'w') as fp:
                #pass

        df = pd.DataFrame(expiry_data_l , columns = columns)

        df.to_csv(path)
    print(datetime.datetime.now())
    time.sleep(180)
def run_option_chain_data_collecter():
    if datetime.datetime.now().hour < 15 and datetime.datetime.now().hour > 9:
        option_chain_data_collecter()
    elif datetime.datetime.now().hour == 15 :
        if datetime.datetime.now().minute <= 30:
            option_chain_data_collecter()
    elif datetime.datetime.now().hour == 9 :
        if datetime.datetime.now().minute >= 15:
            option_chain_data_collecter()
    else:
        print("Market is closed now. Please run this program during market hours.")

while True:
    try:
        run_option_chain_data_collecter()
    except:
        time.sleep(60)
        try: 
            run_option_chain_data_collecter()
        except:
            break
            print("error")

'''
data["records"]["data"] =[{'strikePrice': 11000, 'expiryDate': '28-Dec-2023', 'CE': {'strikePrice': 11000, 'expiryDate': '28-Dec-2023', 'underlying': 'NIFTY', 'identifier': 
'OPTIDXNIFTY28-12-2023CE11000.00', 'openInterest': 15, 'changeinOpenInterest': 0, 'pchangeinOpenInterest': 0, 'totalTradedVolume': 0, 'impliedVolatility': 0, 'lastPrice': 0, 
'change': 0, 'pChange': 0, 'totalBuyQuantity': 0, 'totalSellQuantity': 50, 'bidQty': 0, 'bidprice': 0, 'askQty': 50, 'askPrice': 9600, 'underlyingValue': 19491.4}}]
'''