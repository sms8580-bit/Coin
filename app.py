from flask import Flask, render_template, jsonify
from coin_recommender import analyze_market
import threading
import time
from datetime import datetime

import requests

app = Flask(__name__)

# Cache for recommendations
cache = {
    "data": [],
    "last_updated": None,
    "last_price_sync": None
}

def update_cache():
    """Background thread to update heavy recommendations every hour."""
    while True:
        try:
            print(f"[{datetime.now()}] Running Full Market Analysis (Hourly)...")
            results = analyze_market()
            cache["data"] = results
            cache["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cache["last_price_sync"] = cache["last_updated"]
            print(f"[{datetime.now()}] Analysis completed and cache updated.")
        except Exception as e:
            print(f"Error in heavy update routine: {e}")
        
        time.sleep(3600)

def live_price_update():
    """Background thread to update prices of recommended coins every 30 seconds."""
    while True:
        if cache["data"]:
            try:
                markets = [item['market'] for item in cache["data"]]
                url = "https://api.upbit.com/v1/ticker"
                params = {"markets": ",".join(markets)}
                res = requests.get(url, params=params)
                tickers_data = res.json()
                
                # Update current prices in cache
                price_map = {item['market']: item['trade_price'] for item in tickers_data}
                
                for reco in cache["data"]:
                    m = reco['market']
                    if m in price_map:
                        reco['current_price'] = price_map[m]
                
                cache["last_price_sync"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # print(f"[{datetime.now()}] Live prices sync completed.")
            except Exception as e:
                print(f"Error in live price update: {e}")
        
        time.sleep(30)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/recommend')
def get_recommendations():
    return jsonify({
        "status": "success",
        "last_updated": cache["last_updated"],
        "last_price_sync": cache["last_price_sync"],
        "recommendations": cache["data"]
    })

if __name__ == '__main__':
    # Start background threads
    threading.Thread(target=update_cache, daemon=True).start()
    threading.Thread(target=live_price_update, daemon=True).start()
    
    print("Starting Web Server at http://127.0.0.1:5000")
    app.run(host='0.0.0.0', port=5000)
