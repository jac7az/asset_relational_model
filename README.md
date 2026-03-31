# DS4320 Project 1: 
* Carissa Chen, jac7az
* DOI: 
* [Press Release](https://github.com/jac7az/asset_relational_model/blob/764e32838acca083df828dc2c4081d29c54b3649/PressRease.md)
* [Data](https://myuva-my.sharepoint.com/:f:/g/personal/jac7az_virginia_edu/IgA1d5U-5iH5T7CmRf6IUeZYAZKdqhgw6eprI8RFAJ2KFwA?e=MJDiqu)
* Pipeline
* License: 
## Problem Definition
The general problem is forecasting market prices, but the specific problem is comparing individual asset yield performance over 24 months against general market trends to illustrate volatility.

The rationale behind this refinement is the problem with focusing on a single type of asset. It's prone to market-wide changes and is volatile. It can be predicted, but a single incident can change future predictions. By refining the focus to comparing ratios across asset types, the approach shifts to comparing financial assets and identifying combinations that are overall more secure and practical to invest in than individual stocks.

Investing is a very popular and common way to expand one's finances, but it can be very difficult and confusing to know what and who to invest in. It's an expansive world even with proper knowledge, and taking on risk always means there are chances for loss, which is not what anyone wants. Investors, new and beginners investers especially, want high returns with financial security, but don't necessarily know where to start or what's best. By using historical data to build a robust model that can narrow down the highest security combinations, investors can make more data-driven, confident decisions without being exposed to unnecessary risk.

[Headline!! New Relational Model Unveils the Hidden Risks of Individual Stock Portfolios](https://github.com/jac7az/asset_relational_model/blob/764e32838acca083df828dc2c4081d29c54b3649/PressRease.md)

## Domain Exposition
| Term          | Definition    |
| ------------- | ------------- |
|Asset Class| Investment categories that are subject to the same laws and regulations, like equities or real estate|
|Bonds|A loan to a government or corporation that pays back with interest, usually the most stable investment option|
|Diversification| Risk management strategy that mixes a variety of investments into a single portfolio|
|Equity|Partial ownership of a company; prices change based on a company's performance relative to the market|
|Exchange-Traded Fund (ETF)|Investment security that can be a combination of diversified investments but tracked and sold like a normal stock exchange |
|Fixed Income| Type of investment where the return is fixed at a certain rate|
|Security|Financial instrument that holds a monetary value like a stock, bond, or ETF|
|Interest|Cost of borrowing money or the return for lending it, typically written as a percentage|
|Portfolio|Group of investment assets, including equity, securities, stock, debt, etc.|
|Return|Money made or lost in an investment over time|
|Risk|Possibility of losing some or all of an original investment, or the chance that the investment's actual return is different from originally planned. Higher risk tends to demand a higher return|
|Ticker Symbol| Unique combination of letters to represent a specific security traded on an exchange|

---
|KPI|Definition|
|-------------|---------------|
|Beta|Measure of an asset's volatility in relation to the overall market to determine a stock or ETF's risk level|
|Compound Annual Growth Rate|Mean annual growth rate of an investment over time
|Dividend Yield|Ratio of cash dividends to the company's share price, expressed as a percentage.
|Maximum Drawdown|Maximum loss from a peak to a trough of a portfolio before a new peak|
|Sharpe Ratio|Measures risk-adjusted return and how much excess return a person receives for volatility endured|
|Standard Deviation|Measures market volatility, representing how much an asset's price deviates from its average
|Volatility|Degree of variation in a security's price over time|

This project is under the domain of finance and asset management, where the goal is to optimize the distribution of capital across various financial strategies to achieve as much return as possible with as low risk as possible. To predict the market conditions, analysts take into account different asset classes like equities and ETFs. The goal of this project is to help find the optimal outcome based on historical trends to grow individual wealth and earnings over time.

[Link to reading materials](https://myuva-my.sharepoint.com/:f:/g/personal/jac7az_virginia_edu/IgA6eL8JDhzgTJ4Z6vFyTISQAce43hse6a698G3H_LWUOdA)
| Article Title | Publishing Site|Link|Brief Description
| ------------- | ------------- |-------|------------------|
|How to Start Investing (Even Small Amounts)|MoneyFit|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQAVKSZAgGB0QZBeQpXWkQC6AcmOzBtmmkVnJkS4U_PaZ0c?e=23hv7h|Explains a basic step-by-step plan to get started on investing for beginners with low-pressure ideas
|Investing Basics: Bonds, Stocks, Mutual Funds and ETFs|Financial Readiness|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQDBYeDGGMEKQrnrFC9o9s_bAbYGmp76WO082yfvZlVV-PU?e=ThREEJ|Explains the basic few types of investments people can do and what they are for financial readiness and fraud protection|
|Is Your Dividend Safe? A Step-by-Step Guide to Dividend Analysis (with Pfizer Case Study)|Bull Biscuit|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQCI20enr3LSSKMyCuO1uxyhAdUitjNOBNxI3xFg5ScRHdY?e=YNonfy|Explains dividend, dividend safety, how to analyze and calculate KPIs related to dividends, and an example with Pfizer|
|Sharpe Ratio: Definition, Formula, and Examples|investopedia|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQBsgeDlpOQ2QoL0_Q8FOhkiAQCd5NifuJ5N6w7MPnuFgjg|Defines sharpe ratio, how to calculate it, and what it's used for
|How to invest your money|Fidelity|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQAVKSZAgGB0QZBeQpXWkQC6AcmOzBtmmkVnJkS4U_PaZ0c?e=orPjYy|Outlines a process for beginning an investment journey and what to do.
|Understanding the Difference Between Stocks, Bonds, ETFs, and Mutual Funds|Stock Dork|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQAq0aQJZjOQQI4sbL8oyrGlAR81-NpuAqT__10kyhxXQaw?e=hO8tOy|Explains in depth the differences between the 4 main investment types
|Trend Analysis 101|Equiti|https://myuva-my.sharepoint.com/:b:/g/personal/jac7az_virginia_edu/IQCYTr02ikO9TLGtNc-MqvYuAeTJkIUzaiNTWSCPnH1D8H8?e=8f6Vcu|Explains how to use historical price data to identify market patterns and predict asset changes to help investors know when to buy or sell and leave.
## Data Creation
The raw data for financial records with stocks, ETFs, mutual funds, and bonds was pulled from Kaggle, ranging from the years 1999-2022. Overall, the main data found was the highest and lowest pricings, closing and opening prices, the company it was for, and the data of this data. This was combined into one table for each asset class and converted into a parquet file to reduce space but maintain its overall integrity across millions of entries without system failures. Stocks and ETFs were ingested as text files, while the other 2 were ingested as CSVs.

|Output File|Brief Description on Logic|Link to Code|
|-----------|------------------------------------------------|------------|
|stocks.parquet|Converted .txt stock files into csv files from years ranging 1999-2022, creating data from ticker symbol, date, highest & lowest price range of the day, closing price, number of shares traded and closing price, and added an asset_id per row formed using the company ticker and the date the data was from| https://github.com/jac7az/asset_relational_model/blob/main/data_creation.py|
|etfs.parquet|Converted .txt ETF files into csv files from years 1999-2022, creating data from ticker symbol, date, highest & lowest price range of the day, closing price, number of shares traded and closing price, and added an asset_id per row formed using the company ticker and the date the data was from|https://github.com/jac7az/asset_relational_model/blob/main/data_creation.py|
|mf.parquet|ingested 3 CSV's with ticker letter starting from F to Z, combined into one table, and added asset_id per row formed using the company ticker and the date the data was from, with years ranging from 1973-2021|https://github.com/jac7az/asset_relational_model/blob/main/data_creation.py|
|bonds.parquet|Ingested a time series CSV containing 30 years of treasury bond data from the US Treasury, representing the baseline risk-free rate. This data does not include any other bond distributor. Instead of a company ticker, asset_id was formed using "yield" and the date the data is from| https://github.com/jac7az/asset_relational_model/blob/main/data_creation.py|

The data files likely do not include any companies that have gone bankrupt in the intervening years. As such, the overall health of the market would look better than it actually might be. Additionally, for mutual funds, due to resource constraints, the files did not ingest data with companies starting with any letter from L to P in their ticker, and after, not including any companies before, missing all data from them. For the treasury dataset, since it only includes US Treasury, any corporate or short-term bonds are not included, treating the bond market as entirely risk-free. 

To account for this, all biases and potential limitations are disclosed, such as "Missing companies L-P'. For the mutual fund dataset specifically, the file with the least amount of data was removed to minimize impact on overall representation. The bonds dataset will only be treated as a baseline of comparison instead of being treated as a diverse portfolio similar to the other 3 datasets. Finally, for stocks and ETF data, future analysis would use the portfolio's performance to ensure alignment with real market trends.

The transactions dataset was formed as synthetic data to create a theoretical snapshot of 2.8million transactions in a day. This was centered on a singular date to ensure that all transactions had a real price table range to compare against, instead of a completely random number through randomization that would've been impossible in reality.
The decision to create an asset ID as a combination is from the observation that a ticker has many dates and dates have many tickers, but combined together, each instance will be unique, allowing for a primary key of that table to be formed. Similarly, with bond data that doesn't have a ticker, "yield" was used together with each unique date.
As mentioned with the mutual fund data, due to resource and memory restrictions, the mutual fund dataset couldn't include all of the data, so the decision was made to cut the file to fit as much data as possible, with dataset L-P being the file that could allow all other data to fit, allowing the rest to be combined.

## Metadata
ERD
<img width="1839" height="992" alt="image" src="https://github.com/user-attachments/assets/dd17b5f6-9e41-4c78-8538-c2712f78515b" />


|Table|Link|Description|
|-----|----|-----------|
|transactions|https://myuva-my.sharepoint.com/:u:/g/personal/jac7az_virginia_edu/IQA_VkZs27M4Tb8e2voJOXHmAVmUkokL7xzfr590du0qBSI?e=YnfbIw|Central table containing 2.8million synthetic trade records for 10/27/2015, linked to each asset by asset_id|
|stocks|https://myuva-my.sharepoint.com/:u:/g/personal/jac7az_virginia_edu/IQCOv0lq0Ur7TJ6KTEL3Tf1fAcA7MwWlWaNcb0piQ8ZfAXs?e=ATY8T7|Containes historical stock price data, the date of information, listing highest & lowest stock price of the day, the starting opening and ending closing price, the volume of stocks traded and the ticker symbol representing the company|
|etfs|https://myuva-my.sharepoint.com/:u:/g/personal/jac7az_virginia_edu/IQBn016fIuNbRIHUq99mfd4oARQwCTAJaCt6UKv5Q9yoJl0?e=z2nC5a|Table for Exchange Traded Funds historical data, including highest and lowest stock price of the day, the date of information, the starting opening and closing price, the volume of ETFs traded and the ticker symbol representing the portfolio|
|bonds|https://myuva-my.sharepoint.com/:u:/g/personal/jac7az_virginia_edu/IQDFZAmmxssbTogH0v0k7Pb2AeViHoCUOS6vUMbC5QIFzGY?e=cWecol|Table for 30-year US Treasury Yield bond data as a risk-free baseline
|mf|https://myuva-my.sharepoint.com/:u:/g/personal/jac7az_virginia_edu/IQCTHdsyDL0jTJhuHdeTZAdAAfR7cuZCOPlOzMGYA63X33Q?e=Hz15rd|Table for mutual funds data, listing date of information, the net asset value per share and ticker symbol of mutual fund|
#### Master Dictionary
|Feature|Data Type|Description|Example|
|-----|----|-----------|---|
|transaction_id|int|Unique identifier for each individual transaction on that specific day|1687621|
|account_id|int|The account number identifier for the user|1543|
|asset_type|categorical|The type of financial asset|Stocks
|asset_id|string|Key for unique asset and date, structured as TickerName_Year-Month-Day|AAPL_2015-10-27
|ticker|string|Official market ticker symbol for the asset|AAPL
|quantity|int|The amount of units or shares traded on the date|196
|price|float|The price the transaction was made at for the asset|32.10
|date|datetime|The date the record was produced|2015-10-27
|nav_per_share|float|Net Asset Value per share used for mutual funds|32.98
|open|float|The initial trading price when the market opens for the day|111.57
|high|float|The highest traded price for the asset on the day|123.78
|low|float|The lowest traded price for the asset on the day|7.23
|close|float|The final price the asset was traded at before the market closed for the day|87.21
|volume|int|The total number of shares or units transacted in the given date|168798431687
|openint|int|Open interest, the total number of outstanding contracts|0
|adj close|float|Closing price for bonds adjusted for corporate actions like dividends|78.21
