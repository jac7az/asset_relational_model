import kagglehub
import os
import glob
import pandas as pd
import numpy as np
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='creation.log'
)
logger = logging.getLogger(__name__)

try: 
  #import data from online source
  logger.info("Starting data download")
  path = kagglehub.dataset_download("borismarjanovic/price-volume-data-for-all-us-stocks-etfs")
  path_mf = kagglehub.dataset_download("stefanoleone992/mutual-funds-and-etfs")
  path_bonds = kagglehub.dataset_download("mukhazarahmad/22-years-of-us-treasury-bonds-data")

  #read data from stocks and etf website, convert to table and save as parquet
  stocks_path=os.path.join(path,"Data","Stocks")
  etfs_path=os.path.join(path,"Data","ETFs")
  def get_tables(file_path):
    files=sorted(glob.glob(os.path.join(file_path, "*.txt")))
    data=[]
    for f in files:
      try:
        temp=pd.read_csv(f)
        temp.columns=[c.strip().lower() for c in temp.columns][:200]
        temp['ticker']=os.path.basename(f).split('.')[0].upper()
        data.append(temp)
      except Exception as e:
        continue
    return pd.concat(data,ignore_index=True)

  logger.info("Processing Stocks and ETFs")
  stocks=get_tables(stocks_path)
  etfs=get_tables(etfs_path)
  stocks['asset_id'] = stocks['ticker'].astype(str) + "_" + stocks['date'].astype(str)
  etfs['asset_id'] = etfs['ticker'].astype(str) + "_" + etfs['date'].astype(str)
  stocks.to_parquet('stocks.parquet')
  etfs.to_parquet('etfs.parquet')

  #read data from mutual fund website, convert to table and save as parquet
  logger.info("Processing Mutual Funds")
  mfae=pd.read_csv(os.path.join(path_mf,"MutualFund prices - A-E.csv"))
  mffk=pd.read_csv(os.path.join(path_mf,"MutualFund prices - F-K.csv"))
  mfqz=pd.read_csv(os.path.join(path_mf,"MutualFund prices - Q-Z.csv"))
  mf = pd.concat([mfae,mffk, mfqz], ignore_index=True)
  mf['asset_id'] = mf['fund_symbol'].astype(str) + "_" + mf['price_date'].astype(str)
  mf=mf.rename(columns={'price_date':'date','fund_symbol':'ticker'})
  mf.to_parquet('mf.parquet')

  #read data from treasury bonds website, convert to table and save as parquet
  logger.info("Processing Bonds")
  bonds=pd.read_csv(os.path.join(path_bonds,"Treasury Yield 30 Years.csv"))
  bonds['asset_id'] = "yield" + "_" + bonds['Date'].astype(str)
  bonds['Date']=pd.to_datetime(bonds['Date'])
  bonds['Date'] = bonds['Date'].dt.strftime('%Y-%m-%d')
  bonds.columns=[c.lower() for c in bonds.columns]
  bonds.to_parquet('bonds.parquet')
except Exception as e:
  logger.error(f"Data pulling and creation failed: {e}")

try:
  #Create transaction table for 10/27/2006, selecting a random asset class and company, random price between the day's highest and lowest as potential transactions that can happen all in the day, then save as parquet
  np.random.default_rng(523)
  n=700000
  logger.info("Generating Stock transactions")
  print("Stocks transaction")
  df=pd.read_parquet('/content/stocks.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'high', 'low', 'date','asset_id'])
  df=df.sample(n=n,replace=True)
  df['price']=np.random.uniform(df['low'],df['high']).round(2)
  df['account_id']=np.random.randint(1000,10000,size=len(df))
  df['quantity']=np.random.randint(1,143,size=len(df))
  df['asset_type']='Stock'
  df.rename(columns={"Date":'date'},inplace=True)
  df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='w',index=False)

  print("ETF transactions")
  logger.info("Generating ETF transactions")
  df=pd.read_parquet('/content/etfs.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'high', 'low', 'date','asset_id'])
  df=df.sample(n=n,replace=True)
  df['price']=np.random.uniform(df['low'],df['high']).round(2)
  df['account_id']=np.random.randint(1000,10000,size=len(df))
  df['quantity']=np.random.randint(1,143,size=len(df))
  df['asset_type']='ETFs'
  df.rename(columns={"Date":'date'},inplace=True)
  df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='a',index=False,header=False)
 
  print("Mutual Funds transactions")
  logger.info("Generating Mutual Fund transactions")
  df=pd.read_parquet('/content/mf.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'nav_per_share', 'date','asset_id'])
  df=df.sample(n=n,replace=True)
  df['price']=np.random.uniform(df['nav_per_share']).round(2)
  df['account_id']=np.random.randint(1000,10000,size=len(df))
  df['quantity']=np.random.randint(1,143,size=len(df))
  df['asset_type']='Mutual_Fund'
  df.rename(columns={"Date":'date','fund_symbol':'ticker'},inplace=True)
  df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='a',index=False,header=False)

  print("Bonds transactions")
  logger.info("Generating Bond transactions")
  df=pd.read_parquet('/content/bonds.parquet',filters=[("date",'==','2006-10-27')],columns=['high', 'low', 'date','asset_id'])
  df=df.sample(n=n,replace=True)
  df['price']=np.random.uniform(df['low'],df['high']).round(2)
  df['account_id']=np.random.randint(1000,10000,size=len(df))
  df['quantity']=np.random.randint(1,143,size=len(df))
  df['asset_type']='Bond'
  df['ticker']='US_TREASURY'
  df.rename(columns={"Date":'date'},inplace=True)
  df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='a',index=False,header=False)

  df=pd.read_csv('/content/transactions.csv')
  df['transaction_id']=range(1,len(df)+1)
  df.to_parquet('transactions.parquet')
  logger.info("Pipeline complete")
except Exceptions as e:
  logger.error(f"Conversion to parquet failed: {e}")