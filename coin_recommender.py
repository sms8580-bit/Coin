import requests
import pandas as pd
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def get_krw_tickers():
    """Fetches all KRW market tickers from Upbit."""
    url = "https://api.upbit.com/v1/market/all"
    params = {"isDetails": "false"}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return [item['market'] for item in data if item['market'].startswith('KRW-')]
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

def get_candles(ticker, interval='minutes/60', count=20):
    """Fetches candles for a given ticker and interval."""
    if interval == 'days':
        url = "https://api.upbit.com/v1/candles/days"
    else:
        url = f"https://api.upbit.com/v1/candles/{interval}"
        
    params = {
        "market": ticker,
        "count": count
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching candles ({interval}) for {ticker}: {e}")
        return []

def calculate_macd(df):
    """Calculates MACD(12, 26, 9)."""
    # EMA 12, 26
    exp12 = df['trade_price'].ewm(span=12, adjust=False).mean()
    exp26 = df['trade_price'].ewm(span=26, adjust=False).mean()
    macd_line = exp12 - exp26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

def check_macd_golden_cross(ticker):
    """Checks if Daily MACD has a golden cross or is rising above signal."""
    candles = get_candles(ticker, interval='days', count=50)
    if not candles or len(candles) < 30:
        return False, 0
    
    df = pd.DataFrame(candles)
    df = df.iloc[::-1].reset_index(drop=True)
    df['trade_price'] = df['trade_price'].astype(float)
    
    macd_line, signal_line, _ = calculate_macd(df)
    
    curr_macd = macd_line.iloc[-1]
    prev_macd = macd_line.iloc[-2]
    curr_sig = signal_line.iloc[-1]
    prev_sig = signal_line.iloc[-2]
    
    # 1. Golden Cross
    is_golden_cross = (prev_macd <= prev_sig) and (curr_macd > curr_sig)
    # 2. Strong Upward
    is_rising = (curr_macd > curr_sig) and (curr_macd > prev_macd)
    
    if is_golden_cross or is_rising:
        return True, curr_macd - curr_sig
    return False, 0

def analyze_single_ticker(ticker_info):
    """Worker function to analyze a single coin's MACD and MA10 status."""
    ticker = ticker_info['market']
    try:
        # 1. Check Daily MACD
        is_macd_ok, macd_strength = check_macd_golden_cross(ticker)
        if not is_macd_ok:
            return None
            
        # 2. Check 1-hour MA10
        candles_1h = get_candles(ticker, interval='minutes/60', count=20)
        if not candles_1h or len(candles_1h) < 12:
            return None
            
        df_1h = pd.DataFrame(candles_1h)
        df_1h = df_1h.iloc[::-1].reset_index(drop=True)
        df_1h['trade_price'] = df_1h['trade_price'].astype(float)
        
        ma10 = df_1h['trade_price'].rolling(window=10).mean()
        curr_ma = ma10.iloc[-1]
        prev_ma = ma10.iloc[-2]
        
        if pd.isna(curr_ma) or pd.isna(prev_ma):
            return None
            
        if curr_ma > prev_ma:
            current_price = df_1h.iloc[-1]['trade_price']
            return {
                'market': ticker,
                'current_price': current_price,
                'ma10': curr_ma,
                'acc_trade_price_24h': ticker_info['acc_trade_price_24h'],
                'slope': curr_ma - prev_ma,
                'normalized_slope': (curr_ma - prev_ma) / curr_ma,
                'macd_strength': macd_strength,
                'buy_price': current_price,
                'tp1': {'price': current_price * 1.015, 'time': '1~4시간'},
                'tp2': {'price': current_price * 1.035, 'time': '4~12시간'},
                'tp3': {'price': current_price * 1.070, 'time': '1~3일'},
                'sl': current_price * 0.99
            }
    except Exception as e:
        print(f"\n[Error] {ticker}: {e}")
    return None

def analyze_market():
    start_time = datetime.now()
    print(f"\n[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] 전수 조사 시작 (병렬 분석)...")
    
    tickers = get_krw_tickers()
    if not tickers: return []
    
    # 1차 필터링
    url = "https://api.upbit.com/v1/ticker"
    valid_tickers = []
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        res = requests.get(url, params={"markets": ",".join(chunk)})
        for item in res.json():
            if -0.02 <= item['signed_change_rate'] <= 0.02: 
                valid_tickers.append({
                    'market': item['market'],
                    'acc_trade_price_24h': item['acc_trade_price_24h']
                })
    
    # 거래량 순 정렬
    valid_tickers.sort(key=lambda x: x['acc_trade_price_24h'], reverse=True)
    print(f"1차 필터링 통과: {len(valid_tickers)}개. 분석 시작...")
    
    final_list = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(analyze_single_ticker, valid_tickers))
        final_list = [r for r in results if r is not None]

    final_list.sort(key=lambda x: x['acc_trade_price_24h'], reverse=True)
    top_5 = final_list[:5]
    
    dur = (datetime.now() - start_time).seconds
    print(f"분석 완료 (소요시간: {dur}초).")
    return top_5

if __name__ == "__main__":
    print("분석 테스트...")
    res = analyze_market()
    print(res)
