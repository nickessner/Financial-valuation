import yfinance as yf
from yahooquery import Ticker
import pandas as pd
import time
import numpy as np
from yahoo_fin import stock_info as si
import unicodedata

dow_tickers = ['MSFT','AAPL','NVDA','AMZN','WMT','JPM','V','PG','JNJ','UNH','HD','KO','CRM','CVX','CSCO','IBM','MCD','MRK','AXP','VZ','GS','DIS','CAT','AMGN','BA','HON','SHW','NKE','MMM','TRV']
sp500_tickers = ['MMM','ACE','ABT','ANF','ACN','ADBE','AMD','AES','AET','AFL','A','GAS','APD','ARG','AKAM','AA','ALXN','ATI','AGN','ALL','ANR','ALTR','MO','AMZN','AEE','AEP','AXP','AIG','AMT','AMP','ABC','AMGN','APH','APC','ADI','AON','APA','AIV','APOL','AAPL','AMAT','ADM','AIZ','T','ADSK','ADP','AN','AZO','AVB','AVY','AVP','BHI','BLL','BAC','BK','BCR','BAX','BBT','BEAM','BDX','BBBY','BMS','BRK.B','BBY','BIG','BIIB','BLK','HRB','BMC','BA','BWA','BXP','BSX','BMY','BRCM','BF.B','CHRW','CA','CVC','COG','CAM','CPB','COF','CAH','CFN','KMX','CCL','CAT','CBG','CBS','CELG','CNP','CTL','CERN','CF','SCHW','CHK','CVX','CMG','CB','CI','CINF','CTAS','CSCO','C','CTXS','CLF','CLX','CME','CMS','COH','KO','CCE','CTSH','CL','CMCSA','CMA','CSC','CAG','COP','CNX','ED','STZ','CBE','GLW','COST','CVH','COV','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DF','DE','DELL','DNR','XRAY','DVN','DV','DO','DTV','DFS','DISCA','DLTR','D','RRD','DOV','DOW','DPS','DTE','DD','DUK','DNB','ETFC','EMN','ETN','EBAY','ECL','EIX','EW','EA','EMC','EMR','ESV','ETR','EOG','EQT','EFX','EQR','EL','EXC','EXPE','EXPD','ESRX','XOM','FFIV','FDO','FAST','FII','FDX','FIS','FITB','FHN','FSLR','FE','FISV','FLIR','FLS','FLR','FMC','FTI','F','FRX','FOSL','BEN','FCX','FTR','GME','GCI','GPS','GD','GE','GIS','GPC','GNW','GILD','GS','GT','GOOG','GWW','HAL','HOG','HAR','HRS','HIG','HAS','HCP','HCN','HNZ','HP','HES','HPQ','HD','HON','HRL','HSP','HST','HCBK','HUM','HBAN','ITW','IR','TEG','INTC','ICE','IBM','IFF','IGT','IP','IPG','INTU','ISRG','IVZ','IRM','JBL','JEC','JDSU','JNJ','JCI','JOY','JPM','JNPR','K','KEY','KMB','KIM','KMI','KLAC','KSS','KFT','KR','LLL','LH','LRCX','LM','LEG','LEN','LUK','LXK','LIFE','LLY','LTD','LNC','LLTC','LMT','L','LO','LOW','LSI','MTB','M','MRO','MPC','MAR','MMC','MAS','MA','MAT','MKC','MCD','MHP','MCK','MJN','MWV','MDT','MRK','MET','PCS','MCHP','MU','MSFT','MOLX','TAP','MON','MNST','MCO','MS','MOS','MSI','MUR','MYL','NBR','NDAQ','NOV','NTAP','NFLX','NWL','NFX','NEM','NWSA','NEE','NKE','NI','NE','NBL','JWN','NSC','NTRS','NOC','NU','NRG','NUE','NVDA','NYX','ORLY','OXY','OMC','OKE','ORCL','OI','PCAR','PLL','PH','PDCO','PAYX','BTU','JCP','PBCT','POM','PEP','PKI','PRGO','PFE','PCG','PM','PSX','PNW','PXD','PBI','PCL','PNC','RL','PPG','PPL','PX','PCP','PCLN','PFG','PG','PGR','PLD','PRU','PEG','PSA','PHM','QEP','PWR','QCOM','DGX','RRC','RTN','RHT','RF','RSG','RAI','RHI','ROK','COL','ROP','ROST','RDC','R','SWY','SAI','CRM','SNDK','SCG','SLB','SNI','STX','SEE','SHLD','SRE','SHW','SIAL','SPG','SLM','SJM','SNA','SO','LUV','SWN','SE','S','STJ','SWK','SPLS','SBUX','HOT','STT','SRCL','SYK','SUN','STI','SYMC','SYY','TROW','TGT','TEL','TE','THC','TDC','TER','TSO','TXN','TXT','HSY','TRV','TMO','TIF','TWX','TWC','TIE','TJX','TMK','TSS','TRIP','TSN','TYC','USB','UNP','UNH','UPS','X','UTX','UNM','URBN','VFC','VLO','VAR','VTR','VRSN','VZ','VIAB','V','VNO','VMC','WMT','WAG','DIS','WPO','WM','WAT','WPI','WLP','WFC','WDC','WU','WY','WHR','WFM','WMB','WIN','WEC','WPX','WYN','WYNN','XEL','XRX','XLNX','XL','XYL','YHOO','YUM','ZMH','ZION']
# nasdaq_tickers = si.tickers_nasdaq()
# filtered_nasdaq = [
#     t for t in nasdaq_tickers
#     if len(t) <= 5 and "." not in t and "-" not in t
# ]

# ---- CONFIGURATIONS ---- #
custom_tickers = [
    'NVDA', 'AMZN', 'META', 'NFLX', 'MA', 'GS', 'PGR', 'HIMS', 'RCL', 'CRWD',
    'ANET', 'GE', 'ISRG', 'LLY', 'PANW', 'TRGP', 'BKR', 'SPOT', 'MSFT', 'UBER',
    'AVGO', 'NOW', 'CYBR', 'DKNG', 'BRK-B', 'ADSK', 'CRM', 'BLK', 'ET', 'KKR',
    'WMT', 'CAVA', 'J', 'FI', 'VRT', 'V', 'NU', 'UNH', 'JPM', 'PSTG', 'NEE',
    'CEG', 'PINS', 'HON', 'MRVL', 'KRYS', 'GOOG', 'ICE', 'SCHW', 'ONON', 'DHI',
    'RTX', 'SMTC', 'LYV', 'FTNT', 'INTU', 'JCI', 'MU', 'CTVA', 'VIST', 'MOH',
    'COST', 'MELI', 'DUOL', 'PLTR', 'APP', 'SFM', 'COUR', 'DRS', 'AAPL', 'TSLA',
    'RDDT', 'SBUX', 'CMG', 'MCD', 'MSTR', 'AXP', 'ABBV', 'AMGN', 'ORCL', 'MRK',
    'ADBE', 'MS', 'SCHW', 'GEV', 'LRCX', 'DASH', 'NOC', 'GEHC', 'HUM', 'LEN',
    'FDX', 'UPS', 'DAL', 'DFS', 'D', 'AEP', 'NKE', 'DIS', 'HOOD', 'SOFI', 'VST',
    'ARM', 'NET', 'BAC', 'IBM', 'IONQ', 'HAL', 'UL', 'LOW', 'VZ', 'COP', 'HAL',
    'COF', 'DE', 'LMT', 'CSCO', 'SO', 'TSM', 'ZTS', 'GEHC', 'FIS', 'LULU', 'KR',
    'TT', 'BSX', 'PWR', 'WM', 'AJG', 'OWL', 'TJX', 'TGT', 'PEP', 'BX', 'PFE', 
    'GD', 'CSX', 'ETN', 'PLD', 'AMT', 'AMD', 'CCL', 'DOW', 'TRU', 'IQV', 
    'RVTY', 'MAS', 'ALLE', 'CNH', 'O', 'DOC', 'FTV', 'EIX', 'ES',
    'FANG', 'ARM', 'TTD', 'OKTA', 'DPZ', 'AFRM', 'SAIL', 'HQY', 'WING', 'ALGN',
    'OKLO', 'BWXT', 'XOM', 'CVX', 'CCJ', 'MP', 'LYSDY', 'NTU.AX', 'ILKAY', 'ARRNF', #DOUG'S ENERGY STONX
    'REEMF','TMRC', 'UURAF', 'NB', 'PWRMF', 'NEXA' #DOUG'S ENERGY STONX
]

print("S&P 500:", len(sp500_tickers))
print("Dow 30:", len(dow_tickers))
print("Custom:", len(custom_tickers))

# Combine ALL, but don't filter anything yet
all_tickers = sp500_tickers + dow_tickers + custom_tickers
# print(all_tickers)
print(f"Total tickers before merge: {len(all_tickers)}")

tickers = list(set(all_tickers))
print(f"Total tickers after merge: {len(tickers)}")

chunk_size = 20
pause_duration = 180  # Pause 3 minutes after each chunk

tariff_impact_median = {'High': 0.95, 'Moderate': 0.97, 'Low': 0.99}
tariff_impact_worst = {'High': 0.85, 'Moderate': 0.92, 'Low': 0.97}

exposures = ['High', 'Moderate', 'Low']

def assign_tariff_exposure(row):
    industry = (row.get('Industry') or '').lower()
    sector = (row.get('Sector') or '').lower()

    if any(keyword in industry for keyword in ['semiconductor', 'hardware', 'machinery', 'auto', 'components']):
        return 'High'
    elif any(keyword in industry for keyword in ['retail', 'apparel', 'consumer', 'food', 'agriculture']):
        return 'Moderate'
    elif any(keyword in industry for keyword in ['software', 'insurance', 'bank', 'utility', 'biotech', 'pharma']):
        return 'Low'
    elif 'energy' in sector:
        return 'Moderate'
    else:
        return 'Moderate'  # fallback default

# ---- PROCESSING ---- #
results = []

for i, ticker in enumerate(tickers):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        yq = Ticker(ticker)

        forward_pe = info.get('forwardPE', None)
        if forward_pe is not None and forward_pe <= 0:
            forward_pe = None

        price_to_book = info.get('priceToBook', None)
        if price_to_book is not None and price_to_book <= 0:
            price_to_book = None

        data = {
            'Ticker': ticker,
            'Company': info.get('shortName', None),
            'Industry': info.get('industry', None),
            'Sector': info.get('sector', None),
            'Market Cap': info.get('marketCap', None),
            'Current Price': info.get('currentPrice', None),
            'Forward P/E': forward_pe,
            'Trailing P/E': info.get('trailingPE', None),
            'Price to Book': price_to_book,
            'EV/EBITDA': info.get('enterpriseToEbitda', None),
            'Debt to Equity Ratio': info.get('debtToEquity', None),
            'Operating Margin %': info.get('operatingMargins', None),
            'Tariff Exposure': np.random.choice(exposures),
            '2019 EPS': None,
            '2020 EPS': None,
            '2021 EPS': None,
            '2022 EPS': None,
            '2023 EPS': None,
            '2024 EPS': None,
            '2025 EPS': None,
            '2026 EPS': None,
            'Average EPS (4Y)': None,
            'Trailing P/E Deviation %': None,
            'Forward P/E Deviation %': None,
            '2024 Revenue': None,
            '2025 Revenue': None,
            '2026 Revenue': None,
            'Calculated PEG Ratio': None,
            'Return on Assets %': info.get('returnOnAssets', None),
            'Free Cashflow': info.get('freeCashflow', None),
            'EBITDA Margin %': info.get('ebitdaMargins', None),
            'Recommendation Key': info.get('recommendationKey', None),
            'Target Mean Price': info.get('targetMeanPrice', None),
            'Total Return 3M %': None,
            'Total Return 1Y %': None,
            'Total Return 3Y %': None,
            'Total Return 5Y %': None
        }

        try:
            # MODULE PULLS
            modules = yq.get_modules('defaultKeyStatistics')
            earnings_trend = yq.get_modules('earningsTrend')
            fin_data = yq.get_modules('financialData')
            cashflow_data = yq.get_modules('cashflowStatementHistory')

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
                # Pull historical EPS (up to 4–5 years back)
                eps_history = []
                for i in range(1, 6):  # 2023 to 2019
                    if i < len(trend_data):
                        estimate = trend_data[i].get('earningsEstimate')
                        year_eps = estimate.get('yearAgoEps') if estimate and 'yearAgoEps' in estimate else None
                        if year_eps is not None:
                            eps_history.append(year_eps)
                            data[f'20{24-i} EPS'] = year_eps
                        else:
                            data[f'20{24-i} EPS'] = None

                # Compute average historical EPS (over however many years we have)
                if eps_history:
                    eps_valid = [e for e in eps_history if e is not None]
                    avg_eps = sum(eps_valid) / len(eps_valid) if eps_valid else None
                    data['Average EPS (4Y)'] = avg_eps

                    price = info.get('currentPrice', None)
                    trailing_pe = info.get('trailingPE', None)
                    forward_pe = info.get('forwardPE', None)

                    # Compare trailing/forward P/E vs implied historical P/E
                    if avg_eps and avg_eps > 0 and price:
                        pe_vs_avg_eps = price / avg_eps

                        if trailing_pe and trailing_pe > 0:
                            data['Trailing P/E Deviation %'] = ((trailing_pe - pe_vs_avg_eps) / pe_vs_avg_eps) * 100

                        if forward_pe and forward_pe > 0:
                            data['Forward P/E Deviation %'] = ((forward_pe - pe_vs_avg_eps) / pe_vs_avg_eps) * 100

            if ticker in fin_data:
                data['2024 Revenue'] = fin_data[ticker].get('totalRevenue', None)

                if pd.isna(data['Return on Assets %']) or not isinstance(data['Return on Assets %'], (int, float)):
                    data['Return on Assets %'] = fin_data[ticker].get('returnOnAssets', None)

                if pd.isna(data['Free Cashflow']) or not isinstance(data['Free Cashflow'], (int, float)):
                    data['Free Cashflow'] = fin_data[ticker].get('freeCashflow', None)

                if pd.isna(data['EBITDA Margin %']) or not isinstance(data['EBITDA Margin %'], (int, float)):
                    data['EBITDA Margin %'] = fin_data[ticker].get('ebitdaMargins', None)

                if pd.isna(data['Recommendation Key']) or not isinstance(data['Recommendation Key'], (str)):
                    data['Recommendation Key'] = fin_data[ticker].get('recommendationKey', None)

                if pd.isna(data['Target Mean Price']) or not isinstance(data['Target Mean Price'], (int, float)):
                    data['Target Mean Price'] = fin_data[ticker].get('targetMeanPrice', None)
            
            if ticker in cashflow_data:
                if pd.isna(data['Free Cashflow']) or not isinstance(data['Free Cashflow'], (int, float)):
                    try:
                        statements = cashflow_data[ticker].get('cashflowStatements', [])
                        if statements:
                            latest = statements[0]
                            cfo = latest.get('totalCashFromOperatingActivities')
                            capex = latest.get('capitalExpenditures')
                            if cfo is not None and capex is not None:
                                data['Free Cashflow'] = cfo - capex
                    except Exception as e:
                        print(f"⚠️  Error computing FCF from cashflowStatements for {ticker}: {e}")

            cashflow_df = stock.cashflow  # DataFrame of yearly cash flow

            if not cashflow_df.empty:
                if pd.isna(data['Free Cashflow']) or not isinstance(data['Free Cashflow'], (int, float)):
                    try:
                        cfo = cashflow_df.loc['Total Cash From Operating Activities'].iloc[0]
                        capex = cashflow_df.loc['Capital Expenditures'].iloc[0]
                        if all(isinstance(x, (int, float)) and not pd.isna(x) for x in [cfo, capex]):
                            data['Free Cashflow'] = cfo - capex
                    except:
                        pass

            # RETURN CALCULATIONS
            hist = stock.history(period="5y")
            if not hist.empty:
                try:
                    data['Total Return 3M %'] = ((hist['Close'].iloc[-1] / hist['Close'].iloc[-63]) - 1) * 100
                except:
                    data['Total Return 3M %'] = None
                try:
                    data['Total Return 1Y %'] = ((hist['Close'].iloc[-1] / hist['Close'].iloc[-252]) - 1) * 100
                except:
                    data['Total Return 1Y %'] = None
                try:
                    data['Total Return 3Y %'] = ((hist['Close'].iloc[-1] / hist['Close'].iloc[-756]) - 1) * 100
                except:
                    data['Total Return 3Y %'] = None
                try:
                    data['Total Return 5Y %'] = ((hist['Close'].iloc[-1] / hist['Close'].iloc[0]) - 1) * 100
                except:
                    data['Total Return 5Y %'] = None

        except Exception as e:
            print(f"Error pulling financials for {ticker}: {e}")

        results.append(data)
        print(f"✅ Pulled data for {ticker}")

    except Exception as e:
        print(f"Error pulling {ticker}: {e}")

    if (i + 1) % chunk_size == 0 and i != 0:
        print(f"\n[PAUSE] Sleeping {pause_duration/60:.1f} minutes after {i+1} tickers...\n")
        time.sleep(pause_duration)

# ---- CREATING DATAFRAME ---- #
df = pd.DataFrame(results)

df['Tariff Exposure'] = df.apply(assign_tariff_exposure, axis=1)

def assign_preferred_valuation(row):
    sector = row.get('Sector', '')
    metric = None

    # Primary logic based on sector
    if sector in ['Financial Services', 'Financial']:
        metric = 'Price to Book'
    elif sector == 'Real Estate':
        metric = 'Price to Book'
    elif sector in ['Energy', 'Materials', 'Industrials']:
        metric = 'EV/EBITDA'
    elif pd.notnull(row.get('Forward P/E')):
        metric = 'Forward P/E'
    elif pd.notnull(row.get('Trailing P/E')):
        metric = 'Trailing P/E'
    elif pd.notnull(row.get('Free Cash Flow Yield %')):
        metric = 'Free Cash Flow Yield %'

    # Fallbacks: PEG (future) then regular PEG
    if metric and pd.notnull(row.get(metric)):
        return metric
    elif pd.notnull(row.get('Calculated PEG Ratio (Future earnings)')):
        return 'Calculated PEG Ratio (Future earnings)'
    elif pd.notnull(row.get('Calculated PEG Ratio')):
        return 'Calculated PEG Ratio'
    else:
        return 'N/A'

# ---- CALCULATE GROWTH RATES ---- #
df['EPS Growth (2024-2025)'] = (df['2025 EPS'] - df['2024 EPS']) / df['2024 EPS'].abs() * 100
df['EPS Growth (2025-2026)'] = (df['2026 EPS'] - df['2025 EPS']) / df['2025 EPS'].abs() * 100
df['Revenue Growth (2024-2025)'] = (df['2025 Revenue'] - df['2024 Revenue']) / df['2024 Revenue'] * 100
df['Revenue Growth (2025-2026)'] = (df['2026 Revenue'] - df['2025 Revenue']) / df['2025 Revenue'] * 100

# Now calculate PEG
df['Calculated PEG Ratio'] = df.apply(
    lambda row: row['Forward P/E'] / row['EPS Growth (2024-2025)'] if (
        pd.notnull(row['Forward P/E']) and row['Forward P/E'] > 0 and
        pd.notnull(row['EPS Growth (2024-2025)']) and row['EPS Growth (2024-2025)'] > 0
    ) else None,
    axis=1
)

# Now calculate PEG for future earnings
df['Calculated PEG Ratio (Future earnings)'] = df.apply(
    lambda row: row['Forward P/E'] / row['EPS Growth (2025-2026)'] if (
        pd.notnull(row['Forward P/E']) and row['Forward P/E'] > 0 and
        pd.notnull(row['EPS Growth (2025-2026)']) and row['EPS Growth (2025-2026)'] > 0
    ) else None,
    axis=1
)

df['Free Cash Flow Yield %'] = (df['Free Cashflow'] / df['Market Cap']) * 100

df['Preferred Valuation Metric'] = df.apply(assign_preferred_valuation, axis=1)

zscore_fields = [
    'Forward P/E',
    'Trailing P/E',
    'Price to Book',
    'EV/EBITDA',
    'Debt to Equity Ratio',
    'Operating Margin %',
    'Trailing P/E Deviation %',
    'Forward P/E Deviation %',
    'Calculated PEG Ratio',
    'Return on Assets %',
    'Free Cash Flow Yield %',
    'Free Cashflow',
    'EBITDA Margin %'
]

for col in zscore_fields:
    if col in df.columns:
        mean = df[col].mean(skipna=True)
        std = df[col].std(skipna=True)
        if std > 0:
            df[f'{col} Z'] = (df[col] - mean) / std
        else:
            df[f'{col} Z'] = None  # Avoid division by 0
    else:
        print(f"⚠️ Column '{col}' not found in DataFrame")

z_cols = [f"{col} Z" for col in zscore_fields]
df[z_cols] = df[z_cols].clip(lower=-3, upper=3)

# ---- APPLY TARIFF SCENARIOS ---- #
def adjust_growth(row, impact_dict):
    exposure = row['Tariff Exposure']
    factor = tariff_impact_median.get(exposure, 1)
    eps_2425 = row['EPS Growth (2024-2025)'] * factor if pd.notnull(row['EPS Growth (2024-2025)']) else None
    eps_2526 = row['EPS Growth (2025-2026)'] * factor if pd.notnull(row['EPS Growth (2025-2026)']) else None
    return pd.Series([eps_2425, eps_2526])

median_eps = df.apply(lambda row: adjust_growth(row, tariff_impact_median), axis=1)
median_eps.columns = ['EPS Growth (2024-2025, Median Tariff)', 'EPS Growth (2025-2026, Median Tariff)']

worst_eps = df.apply(lambda row: adjust_growth(row, tariff_impact_worst), axis=1)
worst_eps.columns = ['EPS Growth (2024-2025, Worst Tariff)', 'EPS Growth (2025-2026, Worst Tariff)']

final_df = pd.concat([df, median_eps, worst_eps], axis=1)

# ---- SAVE FINAL FILE ---- #
output_path = 'final_full_stock_model.xlsx'
final_df.to_excel(output_path, index=False)

print(f"\n✅ Completed and saved final enriched dataset to {output_path}")
