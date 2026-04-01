```
import duckdb
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging

# Set up logging to a file to track pipeline execution
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='pipeline.log'
)
# Initialize the logger instance for this specific module
logger = logging.getLogger(__name__)

try: 
    # Create a DataFrame to store the tickers and quantity of shares in the mock portfolio
    holdings_df = pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'GOOG', 'DRYS'],
        'shares_owned': [50, 20, 10, 100]
    })
    logger.info("Portfolio holdings defined.")

    # Connect to DuckDB database 
    con = duckdb.connect(database='portfolio_analysis.duckdb')
    logger.info("DuckDB connection established.")

    # Use read_parquet to load external data files into SQL tables within DuckDB
    con.execute("CREATE TABLE IF NOT EXISTS bonds AS SELECT * FROM read_parquet('bonds.parquet')")
    con.execute("CREATE TABLE IF NOT EXISTS etfs AS SELECT * FROM read_parquet('etfs.parquet')")
    # Insert holdings_df as a table in the DuckDB 
    con.execute("CREATE TABLE IF NOT EXISTS holdings AS SELECT * FROM holdings_df")
    con.execute("CREATE TABLE IF NOT EXISTS stocks AS SELECT * FROM read_parquet('stocks.parquet')")
    logger.info("All relational tables loaded into DuckDB.")

    # Construct the secondary dataset by joining tables and aggregating values per date
    con.execute("""
        CREATE OR REPLACE TABLE secondary_data AS
        SELECT
            s.date,
            SUM(s.close * h.shares_owned) AS portfolio_value, -- Calculate total portfolio value
            AVG(b."Adj Close") AS bond_rate,                  -- Average bond rates for the given date
            AVG(e.close) AS market_benchmark                -- Average ETF price as a market baseline
        FROM stocks s
        JOIN bonds b ON s.date = b.date
        JOIN etfs e ON s.date = e.date
        JOIN holdings h ON s.ticker = h.ticker
        WHERE s.date >= '2016-01-01'
        GROUP BY s.date
    """)
    # Retrieve the constructed secondary table as a pandas DataFrame for modeling
    df = con.execute("SELECT * FROM secondary_data ORDER BY date").df().dropna()
    logger.info(f"Secondary dataset created with {len(df)} records.")

    # Split the dataset into features (bond/market) and the target variable (portfolio value)
    X = df[['bond_rate', 'market_benchmark']]
    y = df['portfolio_value']
    
    # StandardScaler to normalize feature values for balanced model coefficients
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Ridge regression to handle variable variance and noise
    model = Ridge(alpha=1.0)
    # Fit the model to find the relationship between macro trends and portfolio performance
    model.fit(X_scaled, y)

    # Print the model's R-squared value to evaluate prediction accuracy
    print(f"Project R-squared: {model.score(X_scaled, y):.4f}")
    logger.info(f"Model trained successfully. R-squared: {model.score(X_scaled, y):.4f}")

    # Ensure the date column is in a proper datetime object for accurate time-series plotting
    df['date'] = pd.to_datetime(df['date'])

    # Create the matplotlib figure for the visualization
    fig, ax = plt.subplots(figsize=(12, 6))
    # Plot the daily portfolio value to visualize historical performance
    ax.plot(df['date'], df['portfolio_value'], label='My Portfolio Value', color='blue', linewidth=2)

    # Apply scaling logic to the market benchmark to align it visually with the portfolio values, so that it's not just 2 straight lines
    market_scaled = df['market_benchmark'] * (df['portfolio_value'].mean() / df['market_benchmark'].mean())
    # Plot the scaled market trend as a dashed line for comparison
    ax.plot(df['date'], market_scaled, label='Market Trend (Forecasted)', color='orange', linestyle='--')

    # Change the x-axis to show ticks for every month to clear up date crowding
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    # Format the X-axis labels to display as "Month Year"
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xticks(rotation=45)
    
    # Add labels and titles
    ax.set_ylabel("Value in USD ($)", fontsize=12)
    ax.set_xlabel("Timeline (Monthly)", fontsize=12)
    ax.set_title(f"Portfolio Performance vs. Market Trend ({df['date'].min().year}-{df['date'].max().year})", fontsize=14)
    ax.legend(loc='upper right')
    ax.grid(True, linestyle='--', alpha=0.5)

    # Use tight_layout to prevent axis labels from being cut off in the final image
    plt.tight_layout()
    # Save the visualization to a file for GitHub repository documentation
    plt.savefig('portfolio_chart.png')
    logger.info("Visualization generated and saved as portfolio_chart.png.")

except Exception as e:
    logger.error(f"Pipeline failed: {e}")
```

### Analysis Rationale
The analysis aims to move beyond price prediction to produce comparative yield performance, where, with individual assets, a single unforeseeable incident is enough to invalidate a long-term forecast. Integrating Bonds with ETFs and stock data for a broader look at market health and compare to an individual's potential portfolio. The tables are loaded into the DuckDB database for efficient combining across the different asset classes, and a secondary data table besides transactions was constructed specifically for this problem, which calculates a daily weighted portfolio against the average market benchmark for comparison. To account for the high volatility issue, ridge regression with an alpha of 1 was applied to prevent overfitting from asset price spikes and dips. Similarly, StandardScaler was used to standardize the different scalings between different asset classes and the portfolio to balance out the model coefficients.

Mutual Funds and transaction tables were excluded for significant missing data and an entirely different structure. While bonds were the risk-free baseline, Stocks & ETFs were originally from the same source, allowing consistent formatting and comparison. The transactions table is a synthetic dataset designed to simulate a high-volume trading day and validate the overall database schema, thus not applicable to this research question.
### Visualization Rationale
The line graph visualization was designed for a clear look at the 2 years. However, due to the massive scaling difference in total dollar value that would make it difficult to visualize on a graph, a standard scaling was used, which was the market benchmark, and a combination of the average portfolio value and the market benchmark. This would prevent the 2 lines from simply looking like straight lines far above or right at the graph because of their difference. The axis labels were converted into months using rotation, MonthLocator, and DateFormatter, to cut down on the label crowding. Finally, this picture is saved as a PNG for further use in reports or reviews.
