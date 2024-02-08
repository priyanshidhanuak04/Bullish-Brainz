from datetime import datetime, timedelta
import subprocess
import pandas as pd


input_Start_time = "09:30:00"
input_End_time = "15:00:00"
instrument = 'NIFTY'
option = 'CE'
Strategy_name = 'EMA_15_CE_strategy'
Qty = 50
start_date = '2023-09-01'
end_date = '2023-09-30'
year= '2023'
candle_time = 15

#mydf_NIFTY_CE_signals_2023.csv
#filtered_mydf_NIFTY_CE_signals_2023.csv
# mytradedf_NIFTY_CE_2023.csv


headers=["DateTime", "Ticker", "ExpiryDT", "Strike", "F&O", "Option", "Volume", "Open", "High", "Low", "Close", "OpenInterest"]
# df =pd.read_csv("FINNIFTY-I.NFO_2020-01-03.csv", header=None).reset_index(drop=True)
# df.columns=headers
# print(df.head(10))
ExeDF=pd.DataFrame(columns=["Symbol", "Pos", "Date", "Strike", "ExpiryDT", "Option", "EnTime", "SPrice", "ExTime","BPrice"])

OptionDf = pd.DataFrame()
PEOptionDf = pd.DataFrame()
FilteredDf = pd.DataFrame()
UnavailDateList = []
count=0; flag=None; curr_date=None
Pprice=0
high= float("-inf")
low = float("inf")


#i=0
def query(**kwargs):
    """
    :param instrument: String
    :param expry_dt: Datetime
    :param strike: numpy int
    :param option_type: CE  PE
    :param start_date: In Datetime
    :param end_date: In Datetime
    """
    # instrument, f_o, expry_dt, strike, option_type, start_date, end_date)
    global ticker, UnavailDateList

    start_date = kwargs['start_date'].strftime("%Y-%m-%d") + 'T' + "09:15:00"
    end_date = kwargs['end_date'].strftime("%Y-%m-%d") + 'T' + "15:30:00"
    if kwargs['f_o'] == 'O':

        print(str(kwargs['strike']))
        
        ticker = (kwargs['instrument'] + kwargs['expiry_dt'].strftime("%y%-m%d") + str(kwargs['strike']) + kwargs[
            'option_type']).upper()  # nfo FOR OHLCV
        
    elif kwargs['f_o'] == 'F':
        ticker = kwargs['instrument'] + (kwargs['end_date'].strftime("%y%b")).upper() + 'FUT'

    print(ticker, start_date, end_date)

    try:
        subprocess.call(["/home/admin/query_client/ohlcv_1sec", ticker, start_date, end_date])

        print(f"{ticker}_"+kwargs['start_date'].strftime("%Y-%m-%d")+"_1sec.csv")
        
        df = pd.read_csv(f"{ticker}_"+kwargs['start_date'].strftime("%Y-%m-%d")+"_1sec.csv", header=None, low_memory=False).reset_index(drop=True)

        df=df.dropna(axis=1)

        df.columns = ['DateTime','Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
        
        df['Time'] = pd.to_datetime((df['DateTime'])).dt.strftime("%H:%M:%S")

        df["Date"] = pd.to_datetime((df['DateTime'])).dt.strftime("%Y-%m-%d")

        df["Open"] = df["Open"]/100
        df["High"] = df["High"]/100
        df["Low"] = df["Low"]/100
        df["Close"] = df["Close"]/100

        if kwargs['f_o'] == 'O':
            df["Strike"] = kwargs['strike']
            df["Option"] = (kwargs['option_type']).upper()
            df["ExpiryDT"] = kwargs['expiry_dt'].strftime("%Y-%m-%d")

        print(df.head())
        subprocess.call(['unlink', ticker +"_"+kwargs['start_date'].strftime("%Y-%m-%d")+"_1sec.csv"])  # This deletes the file from storage after reading it to memory
        
        return df
    
    except Exception as e:

        print("Exception occured",e)
        df=pd.DataFrame()
        date = kwargs['start_date'].strftime("%Y-%m-%d")
        if date not in UnavailDateList:
            UnavailDateList.append(date)
        print(UnavailDateList)
        subprocess.call(['unlink', ticker +"_"+kwargs['start_date'].strftime("%Y-%m-%d")+"_1sec.csv"])
        return df
    
#---------------------------------------------------------------------------------------------------
def get_expiry(date):

    ExpDf = pd.read_excel("NIFTYData_202309.xls")

    ExpDf["DataTime"] = pd.to_datetime(ExpDf["DataTime"])

    date=pd.to_datetime(date)

    mask = ExpDf["DataTime"] >= date
    
    next_greater_index = mask.idxmax()

    next_greater_date_row = ExpDf.loc[next_greater_index]

    return next_greater_date_row["DataTime"]

def future_data_fn():

    future = query(f_o='F', instrument=instrument, start_date=pd.to_datetime(start_date),
                      end_date=pd.to_datetime(end_date), STime="09:15:00")
    # future = pd.read_csv("futureSep2023.csv")
    print(future)

    future['Timestamp'] = pd.to_datetime(future['DateTime'])
     # Convert 'DateTime' to datetime format
    future.set_index('DateTime', inplace=True)  # Set 'DateTime' as the index
     # Resample data to 15 minute candle
    future_data = future.set_index('Timestamp').resample(f'{candle_time}T').agg({

        'Date': 'first',
        'Time': 'first',
        'Ticker': 'first',
        'Volume': 'sum',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    })
    future_data.reset_index(inplace=True)
    future_data = future_data.dropna(subset=['Date'])

    return future_data

def calculate_EMA_with_signals(df):
    df['EMA_15'] = df["Close"].ewm(span=15, adjust=False).mean()

    start_time = pd.to_datetime(input_Start_time).time()
    end_time = pd.to_datetime(input_End_time).time()
    
    # Assuming df is your DataFrame
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    future_data = df.between_time(start_time, end_time)
    
    # DataFrame index  reset 
    future_data = future_data.reset_index(drop=True)
  
    # Create 'signal' and 'ATMSP' columns if they don't exist
    if 'signal' not in future_data.columns:
        future_data['signal'] = ''
    if 'ATMSP' not in future_data.columns:
        future_data['ATMSP'] = 0
    
    for index, row in future_data.iterrows():

        if index > 0:
  
            if ((future_data['Low'][index - 1] > row['Low']) and (row['Close'] < row['EMA_15']) and (future_data['Close'][index - 1] >= future_data['EMA_15'][index - 1])) or (pd.to_datetime(row['Time']).time() == end_time):
                future_data.at[index, 'signal'] = 'Sell'
            
            elif (row['High'] > future_data['High'][index - 1]) and (row['Close'] > row['EMA_15'])and (future_data['Close'][index - 1] <= future_data['EMA_15'][index - 1]) and (pd.to_datetime(row['Time']).time() < end_time):
                future_data.at[index, 'signal'] = 'Buy'
                future_data.at[index, 'ATMSP'] = round(row['Close'] / 100) * 100     
        else:
            print("Not enough data for calculation.")

    future_data.to_csv(f"mydf_{instrument}_{option}_signals_{year}.csv")

    future_data = future_data[(future_data['signal'] != '')]

     # Group by 'Date' and check if the first row in each group has a 'Sell' signal
    future_data = future_data.groupby('Date').apply(lambda group: group.iloc[1:] if group.iloc[0]['signal'] == 'Sell' else group).reset_index(drop=True)
    
    # Reset the index
    future_data.reset_index(inplace=True)
    
    # Drop the index column
    future_data = future_data.drop(columns=['index'])
    
    
    future_data.to_csv(f"filtered_mydf_{instrument}_{option}_signals_{year}.csv")

    
    return future_data

def main(future_data):    
    OptionCE = pd.DataFrame()
    new_data_list = []# List to store new_data dictionaries
    for index, row in future_data.iterrows():
        if row['signal'] == 'Buy':
            # Create a dictionary with the desired values
            buy_atmsp = row['ATMSP']
            buy_date = row['Date']
            buy_time = (pd.to_datetime(row['Time']) + timedelta(minutes=int(candle_time))).strftime('%H:%M:%S')

            expiry_date = get_expiry(buy_date)

            if all([buy_atmsp, buy_date, buy_time, expiry_date]):
                # Fetch option data
                OptionCE = query(f_o='O', instrument=instrument, expiry_dt=expiry_date, strike=row['ATMSP'], option_type=option, start_date=pd.to_datetime(pd.to_datetime(row['Date']).date()), end_date=pd.to_datetime(pd.to_datetime(row['Date']).date()))

                # Check if option data is not empty
                if not OptionCE.empty:
                    # Extract option data at the same datetime
                    option_buy = OptionCE[OptionCE['Time'] == buy_time]
                    option_buyprice = 0   # Replace with a suitable default value

                    # Check if option data at the same datetime is not empty
                    if not option_buy.empty:
                        option_buyprice = option_buy['Close'].values[0]
                    if option_buyprice >= 10:
                        # Check if the next row is a 'Sell' signal
                        next_row = future_data.iloc[index + 1] if index + 1 < len(future_data) else None
                        if next_row is not None and next_row['signal'] == 'Sell':

                            sell_time = (pd.to_datetime(next_row['Time']) + timedelta(minutes=int(candle_time))).strftime('%H:%M:%S')

                            # Extract option data at the same datetime
                            option_sell = OptionCE[OptionCE['Time'] == sell_time]
                            option_sellprice = 0   # Replace with a suitable default value

                            # Check if option data at the same datetime is not empty
                            if not option_sell.empty:
                                option_sellprice = option_sell['Close'].values[0]
                            

                            new_data = {   
                                    'Strategy': Strategy_name,
                                    'Symbol': instrument,
                                    'Date': buy_date,
                                    'option':option,
                                    'Qty': Qty,
                                    'ExpiryDt': expiry_date,
                                    'Strike': buy_atmsp,
                                    'EnTime': buy_time,
                                    'BPrice': option_buyprice,
                                    'ExTime': sell_time,
                                    'SPrice':option_sellprice,
                                    }
                            
                            # Create a new DataFrame for new_data and concatenate it with the original OptionCE DataFrame
                            new_data_list.append(new_data)
                else:
                    print("Option data is empty for Buy signal.")
  
        else:
            print("Skipping row because the signal is not 'Buy'.")
    return new_data_list 


df = future_data_fn()
sigdf = calculate_EMA_with_signals(df)
tradelist = main(sigdf)
tradedf = pd.DataFrame(tradelist)
tradedf.to_csv(f"mytradedf_{instrument}_{option}_{year}.csv")

