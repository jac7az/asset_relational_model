import duckdb
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging

#Create someone's potential portfolio and establish logging file
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='pipeline.log'
)
logger = logging.getLogger(__name__)
try: 
  holdings_df = pd.DataFrame({
      'ticker': ['AAPL', 'MSFT', 'GOOG', 'DRYS'],
      'shares_owned': [50, 20, 10, 100]
  })
  logger.info("Portfolio holdings defined.")

  con = duckdb.connect(database='portfolio_analysis.duckdb')
  logger.info("DuckDB connection established.")

  # Load Fact Tables from Parquet
  con.execute("CREATE TABLE IF NOT EXISTS bonds AS SELECT * FROM read_parquet('bonds.parquet')")
  con.execute("CREATE TABLE IF NOT EXISTS etfs AS SELECT * FROM read_parquet('etfs.parquet')")
  con.execute("CREATE TABLE IF NOT EXISTS holdings AS SELECT * FROM holdings_df")
  con.execute("CREATE TABLE IF NOT EXISTS stocks AS SELECT * FROM read_parquet('stocks.parquet')")
  logger.info("All relational tables loaded into DuckDB.")


  # Create a secondary dataset filtering for 10 years out of the 3 datasets
  con.execute("""
      CREATE OR REPLACE TABLE secondary_data AS
      SELECT
          s.date,
          SUM(s.close * h.shares_owned) AS portfolio_value,
          AVG(b."Adj Close") AS bond_rate,
          AVG(e.close) AS market_benchmark
      FROM stocks s
      JOIN bonds b ON s.date = b.date
      JOIN etfs e ON s.date = e.date
      JOIN holdings h ON s.ticker = h.ticker
      WHERE s.date >= '2016-01-01'
      GROUP BY s.date
  """)
  df = con.execute("SELECT * FROM secondary_data ORDER BY date").df().dropna()
  logger.info(f"Secondary dataset created with {len(df)} records.")

  #Define Features and Target, standardize for high variable variance, implement model with ridge penalization regularization, and find final results.
  X = df[['bond_rate', 'market_benchmark']]
  y = df['portfolio_value']
  scaler = StandardScaler()
  X_scaled = scaler.fit_transform(X)
  model = Ridge(alpha=1.0)
  model.fit(X_scaled, y)

  print(f"Project R-squared: {model.score(X_scaled, y):.4f}")
  logger.info(f"Model trained successfully. R-squared: {model.score(X_scaled, y):.4f}")

  # Convert date to datetime format, create a plot over the available years 2016-2017
  df['date'] = pd.to_datetime(df['date'])

  fig, ax = plt.subplots(figsize=(12, 6))
  ax.plot(df['date'], df['portfolio_value'], label='My Portfolio Value', color='blue', linewidth=2)

  # Scaling the market trend to match the portfolio's visual height
  market_scaled = df['market_benchmark'] * (df['portfolio_value'].mean() / df['market_benchmark'].mean())
  ax.plot(df['date'], market_scaled, label='Market Trend (Forecasted)', color='orange', linestyle='--')

  ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
  plt.xticks(rotation=45)
  ax.set_ylabel("Value in USD ($)", fontsize=12)
  ax.set_xlabel("Timeline (Monthly)", fontsize=12)
  ax.set_title(f"Portfolio Performance vs. Market Trend ({df['date'].min().year}-{df['date'].max().year})", fontsize=14)
  ax.legend(loc='upper right')
  ax.grid(True, linestyle='--', alpha=0.5)

  plt.tight_layout()
  logger.info("Visualization generated and saved as portfolio_chart.png.")

except Exception as e:
  logger.error(f"Pipeline failed{e}")
