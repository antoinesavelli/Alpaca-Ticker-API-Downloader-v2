"""
Test script to verify if Alpaca has historical data for delisted stocks.
"""
import asyncio
import httpx
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv('keys.env')


async def test_delisted_symbols():
    """Test if Alpaca returns data for known delisted stocks."""
    
    # Known delisted stocks with their delisting info
    test_symbols = {
        'BBBY': {'name': 'Bed Bath & Beyond', 'delisted': '2023-05', 'was_active': '2020-01-01'},
        'SHLD': {'name': 'Sears Holdings', 'delisted': '2018-10', 'was_active': '2017-01-01'},
        'CHK': {'name': 'Chesapeake Energy (old)', 'delisted': '2020-07', 'was_active': '2019-01-01'},
        'HMNY': {'name': 'Helios and Matheson (MoviePass)', 'delisted': '2019-01', 'was_active': '2018-01-01'},
        'DRYS': {'name': 'DryShips', 'delisted': '2016-11', 'was_active': '2016-01-01'},
    }
    
    # Alpaca API setup
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    
    if not api_key or not secret_key:
        print("❌ Missing API keys in keys.env")
        return
    
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "accept": "application/json"
    }
    
    base_url = "https://data.alpaca.markets/v2/stocks/bars"
    
    print("="*80)
    print("TESTING DELISTED SYMBOLS ON ALPACA")
    print("="*80)
    print("\nChecking if Alpaca has historical data for known delisted stocks...")
    print("This will test dates when the symbols WERE actively trading.\n")
    
    results = {
        'has_data': [],
        'no_data': []
    }
    
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        for symbol, info in test_symbols.items():
            print(f"\n{'='*80}")
            print(f"Testing: {symbol} - {info['name']}")
            print(f"  Delisted: {info['delisted']}")
            print(f"  Testing date range: {info['was_active']} (when it WAS trading)")
            print(f"{'='*80}")
            
            # Test 1: Check if symbol exists in Assets API
            assets_url = "https://data.alpaca.markets/v2/assets"
            try:
                assets_response = await client.get(f"{assets_url}/{symbol}")
                if assets_response.status_code == 200:
                    asset_info = assets_response.json()
                    print(f"✓ Symbol found in Assets API")
                    print(f"  Status: {asset_info.get('status')}")
                    print(f"  Tradable: {asset_info.get('tradable')}")
                else:
                    print(f"✗ Symbol NOT in Assets API (status: {assets_response.status_code})")
            except Exception as e:
                print(f"✗ Assets API error: {e}")
            
            # Test 2: Try to fetch historical bars
            params = {
                "symbols": symbol,
                "timeframe": "1Day",
                "start": info['was_active'],
                "end": info['was_active'].replace('01-01', '01-31'),
                "limit": 100,
                "feed": "sip"
            }
            
            try:
                response = await client.get(base_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    bars = data.get('bars', {}).get(symbol, [])
                    
                    if bars and len(bars) > 0:
                        print(f"✅ SUCCESS: Found {len(bars)} historical bars")
                        print(f"   First bar: {bars[0].get('t', 'N/A')}")
                        print(f"   Close price: ${bars[0].get('c', 0):.2f}")
                        results['has_data'].append(symbol)
                    else:
                        print(f"❌ NO DATA: API returned empty bars")
                        results['no_data'].append(symbol)
                elif response.status_code == 422:
                    print(f"❌ INVALID: Symbol not recognized (422)")
                    results['no_data'].append(symbol)
                else:
                    print(f"❌ ERROR: Status {response.status_code}")
                    print(f"   Response: {response.text[:200]}")
                    results['no_data'].append(symbol)
                    
            except Exception as e:
                print(f"❌ REQUEST FAILED: {e}")
                results['no_data'].append(symbol)
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nSymbols WITH historical data: {len(results['has_data'])}")
    for s in results['has_data']:
        print(f"  ✓ {s}")
    
    print(f"\nSymbols WITHOUT historical data: {len(results['no_data'])}")
    for s in results['no_data']:
        print(f"  ✗ {s}")
    
    print("\n" + "="*80)
    print("CONCLUSION")
    print("="*80)
    if len(results['has_data']) > 0:
        print("✅ Alpaca DOES have some delisted stock data")
        print("   Your data will include symbols that WERE delisted")
        print("   as long as they're still in Alpaca's database")
    else:
        print("❌ Alpaca does NOT have delisted stock data")
        print("   Only currently active symbols are available")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(test_delisted_symbols())
