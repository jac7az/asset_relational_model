import kagglehub
import os
import glob
import pandas as pd
import numpy as np
import logging

# logging to 'creation.log' 
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='creation.log',
    force=True # Force override in case Colab root logger is active
)
logger = logging.getLogger(__name__)

try: 
    # Download 3 datasets from Kaggle to load in data
    logger.info("Starting data download")
    path = kagglehub.dataset_download("borismarjanovic/price-volume-data-for-all-us-stocks-etfs")
    path_mf = kagglehub.dataset_download("stefanoleone992/mutual-funds-and-etfs")
    path_bonds = kagglehub.dataset_download("mukhazarahmad/22-years-of-us-treasury-bonds-data")

    # Define paths for the stock and ETF folders with text files
    stocks_path=os.path.join(path,"Data","Stocks")
    etfs_path=os.path.join(path,"Data","ETFs")
    
    # Define a function to iterate through directories and combine text files into a single DataFrame
    def get_tables(file_path):
        files=sorted(glob.glob(os.path.join(file_path, "*.txt")))
        data=[]
        for f in files:
            try:
                temp=pd.read_csv(f)
                # Clean column names and limit feature count for memory efficiency
                temp.columns=[c.strip().lower() for c in temp.columns][:200]
                # Extract ticker symbol from the filename
                temp['ticker']=os.path.basename(f).split('.')[0].upper()
                data.append(temp)
            except Exception as e:
                continue
        # Concatenate all individual stock/ETF dataframes into one large table
        return pd.concat(data,ignore_index=True)

    logger.info("Processing Stocks and ETFs")
    stocks=get_tables(stocks_path)
    etfs=get_tables(etfs_path)
    
    # Create a unique 'asset_id' as a primary key for relational joining (ticker + date)
    stocks['asset_id'] = stocks['ticker'].astype(str) + "_" + stocks['date'].astype(str)
    etfs['asset_id'] = etfs['ticker'].astype(str) + "_" + etfs['date'].astype(str)
    
    # Save processed raw data to Parquet format 
    stocks.to_parquet('stocks.parquet')
    etfs.to_parquet('etfs.parquet')

    logger.info("Processing Mutual Funds")
    # Load separate mutual fund CSV segments into a single table
    mfae=pd.read_csv(os.path.join(path_mf,"MutualFund prices - A-E.csv"))
    mffk=pd.read_csv(os.path.join(path_mf,"MutualFund prices - F-K.csv"))
    mfqz=pd.read_csv(os.path.join(path_mf,"MutualFund prices - Q-Z.csv"))
    mf = pd.concat([mfae,mffk, mfqz], ignore_index=True)
    
    # Create asset_id and clean column names to match overall schema
    mf['asset_id'] = mf['fund_symbol'].astype(str) + "_" + mf['price_date'].astype(str)
    mf=mf.rename(columns={'price_date':'date','fund_symbol':'ticker'})
    mf.to_parquet('mf.parquet')

    logger.info("Processing Bonds")
    # Load Treasury Bond yield data
    bonds=pd.read_csv(os.path.join(path_bonds,"Treasury Yield 30 Years.csv"))
    bonds['asset_id'] = "yield" + "_" + bonds['Date'].astype(str)
    # Convert date strings to standardized format
    bonds['Date']=pd.to_datetime(bonds['Date'])
    bonds['Date'] = bonds['Date'].dt.strftime('%Y-%m-%d')
    bonds.columns=[c.lower() for c in bonds.columns]
    bonds.to_parquet('bonds.parquet')
    
except Exception as e:
    logger.error(f"Data pulling and creation failed: {e}")

try:
    # Use a specific seed for random generation to ensure project reproducibility
    np.random.default_rng(523)
    # Define the scale of the generated transactions table (700,000 rows)
    n=700000
    
    # Filter for a specific date and sample to create mock trade data starting with stocks
    logger.info("Generating Stock transactions")
    df=pd.read_parquet('/content/stocks.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'high', 'low', 'date','asset_id'])
    df=df.sample(n=n,replace=True)
    # Generate random transaction prices between the day's high and low values
    df['price']=np.random.uniform(df['low'],df['high']).round(2)
    df['account_id']=np.random.randint(1000,10000,size=len(df))
    df['quantity']=np.random.randint(1,143,size=len(df))
    df['asset_type']='Stock'
    df.rename(columns={"Date":'date'},inplace=True)
    # Write to CSV initially for a buffer
    df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='w',index=False)

    # Append ETF trades to the transaction list
    logger.info("Generating ETF transactions")
    df=pd.read_parquet('/content/etfs.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'high', 'low', 'date','asset_id'])
    df=df.sample(n=n,replace=True)
    df['price']=np.random.uniform(df['low'],df['high']).round(2)
    df['account_id']=np.random.randint(1000,10000,size=len(df))
    df['quantity']=np.random.randint(1,143,size=len(df))
    df['asset_type']='ETFs'
    df.rename(columns={"Date":'date'},inplace=True)
    df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='a',index=False,header=False)
     
    # Append MF trades using NAV per share as the price
    logger.info("Generating Mutual Fund transactions")
    df=pd.read_parquet('/content/mf.parquet',filters=[("date",'==','2006-10-27')],columns=['ticker', 'nav_per_share', 'date','asset_id'])
    df=df.sample(n=n,replace=True)
    df['price']=np.random.uniform(df['nav_per_share']).round(2)
    df['account_id']=np.random.randint(1000,10000,size=len(df))
    df['quantity']=np.random.randint(1,143,size=len(df))
    df['asset_type']='Mutual_Fund'
    df.rename(columns={"Date":'date','fund_symbol':'ticker'},inplace=True)
    df[['account_id','asset_type','asset_id','ticker','price','quantity']].to_csv('transactions.csv',mode='a',index=False,header=False)

    # Append Bond trades representing Treasury yields
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

    # Load full csv and add a unique transaction_id as a primary key
    df=pd.read_csv('/content/transactions.csv')
    df['transaction_id']=range(1,len(df)+1)
    # Convert final transactions table to Parquet 
    df.to_parquet('transactions.parquet')
    logger.info("Pipeline complete")
    
except Exception as e:
    logger.error(f"Conversion to parquet failed: {e}")