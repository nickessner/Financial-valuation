import yfinance as yf
from yahooquery import Ticker
import pandas as pd
import time
import numpy as np

# ---- CONFIGURATIONS ---- #
tickers = [
    'NVDA', 'AMZN', 'META', 'NFLX', 'MA', 'GS', 'PGR', 'HIMS', 'RCL', 'CRWD',
    'ANET', 'GE', 'ISRG', 'LLY', 'PANW', 'TRGP', 'BKR', 'SPOT', 'MSFT', 'UBER',
    'AVGO', 'NOW', 'CYBR', 'DKNG', 'BRK/B', 'ADSK', 'CRM', 'BLK', 'ET', 'KKR',
    'WMT', 'CAVA', 'J', 'FI', 'VRT', 'V', 'GOOGL', 'NU', 'UNH', 'JPM', 'PSTG',
    'CEG', 'PINS', 'HON', 'MRVL', 'KRYS', 'GOOG', 'ICE', 'SCHW', 'ONON', 'DHI',
    'RTX', 'SMTC', 'LYV', 'FTNT', 'INTU', 'JCI', 'MU', 'CTVA', 'VIST', 
    'COST', 'MELI', 'DUOL', 'PLTR', 'APP', 'SFM', 'COUR', 'DRS'
]
chunk_size = 10
pause_duration = 180  # Pause 3 minutes after chunk

# Tariff impact assumptions
tariff_impact_median = {'High': 0.95, 'Moderate': 0.97, 'Low': 0.99}
tariff_impact_worst = {'High': 0.85, 'Moderate': 0.92, 'Low': 0.97}

exposures = ['High', 'Moderate', 'Low']

# ---- PROCESSING ---- #
results = []

for i, ticker in enumerate(tickers):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        yq = Ticker(ticker)

        data = {
            'Ticker': ticker,
            'Company': info.get('shortName', None),
            'Forward P/E': info.get('forwardPE', None),
            'Trailing P/E': info.get('trailingPE', None),
            '2024 EPS': None,
            '2025 EPS': None,
            '2026 EPS': None,
            '2024 Revenue': None,
            '2025 Revenue': None,
            '2026 Revenue': None,
            'Tariff Exposure': np.random.choice(exposures)
        }

        # Pull modules individually
        modules = yq.get_modules('defaultKeyStatistics')
        earnings_trend = yq.get_modules('earningsTrend')
        fin_data = yq.get_modules('financialData')

        if ticker in modules:
            data['2025 EPS'] = modules[ticker].get('forwardEps', None)
            data['2024 EPS'] = modules[ticker].get('trailingEps', None)

        if ticker in earnings_trend:
            trend_data = earnings_trend[ticker].get('trend', [])
            if len(trend_data) >= 3:
                data['2025 Revenue'] = trend_data[2]['revenueEstimate'].get('avg', None)
                data['2025 EPS'] = trend_data[2]['earningsEstimate'].get('avg', data['2025 EPS'])
            if len(trend_data) >= 4:
                data['2026 Revenue'] = trend_data[3]['revenueEstimate'].get('avg', None)
                data['2026 EPS'] = trend_data[3]['earningsEstimate'].get('avg', None)

        if ticker in fin_data:
            data['2024 Revenue'] = fin_data[ticker].get('totalRevenue', None)

        results.append(data)
        print(f"✅ Pulled data for {ticker}")

    except Exception as e:
        print(f"Error pulling {ticker}: {e}")

    if (i + 1) % chunk_size == 0 and i != 0:
        print(f"\n[PAUSE] Sleeping {pause_duration/60:.1f} minutes after {i+1} tickers...\n")
        time.sleep(pause_duration)

# ---- CREATING DATAFRAME ---- #
df = pd.DataFrame(results)

# ---- CALCULATE GROWTH RATES ---- #
df['EPS Growth (2024-2025)'] = (df['2025 EPS'] - df['2024 EPS']) / df['2024 EPS'].abs() * 100
df['EPS Growth (2025-2026)'] = (df['2026 EPS'] - df['2025 EPS']) / df['2025 EPS'].abs() * 100
df['Revenue Growth (2024-2025)'] = (df['2025 Revenue'] - df['2024 Revenue']) / df['2024 Revenue'] * 100
df['Revenue Growth (2025-2026)'] = (df['2026 Revenue'] - df['2025 Revenue']) / df['2025 Revenue'] * 100

# ---- APPLY TARIFF SCENARIOS ---- #
def adjust_growth(row, impact_dict):
    exposure = row['Tariff Exposure']
    factor = tariff_impact_median.get(exposure, 1)
    eps_2425 = row['EPS Growth (2024-2025)'] * factor if pd.notnull(row['EPS Growth (2024-2025)']) else None
    eps_2526 = row['EPS Growth (2025-2026)'] * factor if pd.notnull(row['EPS Growth (2025-2026)']) else None
    return pd.Series([eps_2425, eps_2526])

# Median Case
median_eps = df.apply(lambda row: adjust_growth(row, tariff_impact_median), axis=1)
median_eps.columns = ['EPS Growth (2024-2025, Median Tariff)', 'EPS Growth (2025-2026, Median Tariff)']

# Worst Case
worst_eps = df.apply(lambda row: adjust_growth(row, tariff_impact_worst), axis=1)
worst_eps.columns = ['EPS Growth (2024-2025, Worst Tariff)', 'EPS Growth (2025-2026, Worst Tariff)']

# Merge all together
final_df = pd.concat([df, median_eps, worst_eps], axis=1)

# ---- SAVE FINAL FILE ---- #
output_path = 'final_stock_fundamentals.xlsx'
final_df.to_excel(output_path, index=False)

print(f"\n✅ Completed and saved full dataset to {output_path}")
