import yfinance as yf
from yahooquery import Ticker
import pandas as pd
import time
import numpy as np

# ---- CONFIGURATIONS ---- #
tickers = ['DKNG', 'SPOT', 'NVDA', 'META', 'MSFT', 'AMZN', 'GOOGL']  # Add your full list here
chunk_size = 10  # How many stocks before pausing
pause_duration = 180  # Pause 3 minutes between chunks (in seconds)

# Tariff impact assumptions
tariff_impact_median = {'High': 0.95, 'Moderate': 0.97, 'Low': 0.99}
tariff_impact_worst = {'High': 0.85, 'Moderate': 0.92, 'Low': 0.97}

# Exposure assignment (could be customized based on your internal knowledge)
exposures = ['High', 'Moderate', 'Low']

# Results list
results = []

# ---- PROCESSING ---- #
for i, ticker in enumerate(tickers):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        # Now use yahooquery for full module data
        yq = Ticker(ticker)
        all_data = yq.all_modules

        print(f"\n🔎 Available Yahooquery fields for {ticker}:")
        print(all_data)

        data = {
            'Ticker': ticker,
            'Current Price': info.get('currentPrice', None),
            'Forward P/E': info.get('forwardPE', None),
            'Trailing P/E': info.get('trailingPE', None),
            'PEG Ratio': info.get('pegRatio', None),
            'Industry': info.get('industry', None),
            'Market Cap': info.get('marketCap', None),
            'Enterprise Value': info.get('enterpriseValue', None),
            'Operating Margin %': info.get('operatingMargins', None),
            'Return on Equity %': info.get('returnOnEquity', None),
            'Gross Margin %': info.get('grossMargins', None),
            'Total Debt': info.get('totalDebt', None),
            'Debt to Equity Ratio': info.get('debtToEquity', None),
            'Tariff Exposure': np.random.choice(exposures),
            '2025 EPS': None,
            '2025 Revenue': None
        }

        try:
            fin_data = yq.financial_data

            if isinstance(fin_data, dict) and ticker in fin_data:
                ticker_data = fin_data[ticker]
                data['2025 EPS'] = ticker_data.get('forwardEps', None)
                current_revenue = ticker_data.get('totalRevenue', None)
                revenue_growth = ticker_data.get('revenueGrowth', None)

                if current_revenue is not None and revenue_growth is not None:
                    data['2025 Revenue'] = current_revenue * (1 + revenue_growth)

        except Exception as e:
            print(f"Error pulling detailed financial data for {ticker}: {e}")

        results.append(data)

        print(f"✅ Pulled data for {ticker}")

    except Exception as e:
        print(f"Error pulling {ticker}: {e}")

    if (i + 1) % chunk_size == 0 and i != 0:
        print(f"\n[PAUSE] Sleeping {pause_duration/60:.1f} minutes after {i+1} tickers...\n")
        time.sleep(pause_duration)

# ---- CREATING DATAFRAME ---- #
df = pd.DataFrame(results)

# ---- SAVE RAW DATA TO EXCEL ---- #
output_path = 'full_raw_financial_data.xlsx'
df.to_excel(output_path, index=False)

print(f"\n✅ Completed and saved full data to {output_path}")
