import requests
import pandas as pd
import time
import schedule
from datetime import datetime

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
    
    # 1. Golden Cross: Prev MACD < Prev Sig AND Curr MACD > Curr Sig
    is_golden_cross = (prev_macd <= prev_sig) and (curr_macd > curr_sig)
    
    # 2. Strong Upward: MACD > Sig AND MACD rising
    is_rising = (curr_macd > curr_sig) and (curr_macd > prev_macd)
    
    if is_golden_cross or is_rising:
        return True, curr_macd - curr_sig
    return False, 0

def analyze_market():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 분석 시작 (일봉 MACD + 1시간봉 10선)...")
    tickers = get_krw_tickers()
    
    # Fetch real-time snapshot for initial filtering
    url = "https://api.upbit.com/v1/ticker"
    chunk_size = 100
    valid_tickers = []
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        markets = ",".join(chunk)
        params = {"markets": markets}
        try:
            res = requests.get(url, params=params)
            data = res.json()
            for item in data:
                # signed_change_rate between -2% and +2%
                if -0.02 <= item['signed_change_rate'] <= 0.02: 
                    valid_tickers.append({
                        'market': item['market'],
                        'acc_trade_price_24h': item['acc_trade_price_24h']
                    })
        except Exception as e:
            print(f"Error fetching snapshot: {e}")
            
    print(f"1차 필터링(-2% ~ +2%) 통과: {len(valid_tickers)}개")
    
    final_list = []
    
    for i, ticker_info in enumerate(valid_tickers):
        ticker = ticker_info['market']
        print(f"\r분석 중 ({i+1}/{len(valid_tickers)}): {ticker}", end="")
        time.sleep(0.12)
        
        # 1. Check Daily MACD Golden Cross / Upward
        is_macd_ok, macd_strength = check_macd_golden_cross(ticker)
        if not is_macd_ok:
            continue
            
        # 2. Check 1-hour MA10 Rising
        candles_1h = get_candles(ticker, interval='minutes/60', count=20)
        if not candles_1h or len(candles_1h) < 12:
            continue
            
        df_1h = pd.DataFrame(candles_1h)
        df_1h = df_1h.iloc[::-1].reset_index(drop=True)
        df_1h['trade_price'] = df_1h['trade_price'].astype(float)
        
        # MA10
        ma10 = df_1h['trade_price'].rolling(window=10).mean()
        curr_ma = ma10.iloc[-1]
        prev_ma = ma10.iloc[-2]
        
        if pd.isna(curr_ma) or pd.isna(prev_ma):
            continue
            
        # Strategy: MACD is good AND 1h MA10 is rising
        if curr_ma > prev_ma:
            current_price = df_1h.iloc[-1]['trade_price']
            final_list.append({
                'market': ticker,
                'current_price': current_price,
                'ma10': curr_ma,
                'acc_trade_price_24h': ticker_info['acc_trade_price_24h'],
                'slope': curr_ma - prev_ma,
                'macd_strength': macd_strength,
                'buy_price': current_price,
                # Tiered TP with estimated times
                'tp1': {'price': current_price * 1.015, 'time': '1~4시간 (단기)'},
                'tp2': {'price': current_price * 1.035, 'time': '4~12시간 (스윙)'},
                'tp3': {'price': current_price * 1.070, 'time': '1~3일 (장기)'},
                'sl': current_price * 0.99   # Stop Loss -1% (Stable)
            })

    print("\n분석 완료. 순위 산정 중...")
    
    for item in final_list:
        # Normalized slope for 1h MA10
        item['normalized_slope'] = item['slope'] / item['ma10']
        
    # Sort by Daily Trade Price (Volume) Descending as requested
    final_list.sort(key=lambda x: x['acc_trade_price_24h'], reverse=True)
    
    top_5 = final_list[:5]
    
    return top_5

def print_results(top_5):
    print("\n" + "="*40)
    print(f"[{datetime.now().strftime('%H:%M')}] 급등 포착 추천 코인 (Top 5)")
    print("기준: 1시간봉 10이평 상승 & 당일 10% 미만 상승")
    print("="*40)
    
    if not top_5:
        print("조건에 맞는 코인이 없습니다.")
    else:
        for idx, coin in enumerate(top_5):
            print(f"{idx+1}. {coin['market']} : {coin['current_price']:,.0f}원 (이평급등강도: {coin['normalized_slope']:.5f})")
    print("="*40 + "\n")

def job():
    try:
        results = analyze_market()
        print_results(results)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("봇이 시작되었습니다. 매 시간마다 추천을 진행합니다.")
    print("첫 실행을 시작합니다...")
    job() # Run once immediately
    
    # Schedule for every hour
    schedule.every().hour.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
