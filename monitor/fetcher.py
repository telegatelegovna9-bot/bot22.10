# fetcher.py (полностью измененный код)
import aiohttp
import pandas as pd
from monitor.logger import log
import asyncio

BYBIT_API = "https://api.bybit.com/v5/market"

async def get_all_futures_tickers():
    try:
        url = f"{BYBIT_API}/tickers?category=linear"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log(f"Ошибка получения тикеров: HTTP {resp.status}, Ответ: {await resp.text()}", level="error")
                    return []
                data = await resp.json()
                if not data or not isinstance(data, dict) or 'result' not in data or 'list' not in data['result']:
                    log(f"Ошибка: данные тикеров не являются списком или пусты: {data}", level="error")
                    return []

            from monitor.settings import load_config
            config = load_config()
            volume_filter = config.get('volume_filter', 5_000_000.0)
            tickers = []
            failed_reasons = {'volume': 0, 'usdt': 0}

            for item in data['result']['list']:
                if not isinstance(item, dict):
                    continue
                symbol = item.get('symbol', '')
                quote_volume = float(item.get('turnover24h', 0))  # Bybit использует 'turnover24h' для объема в USDT

                if not symbol.endswith('USDT'):
                    failed_reasons['usdt'] += 1
                    continue
                if quote_volume < volume_filter:
                    failed_reasons['volume'] += 1
                    continue

                tickers.append(symbol)

            log(f"Всего тикеров: {len(data['result']['list'])}, после фильтра по объёму ({volume_filter}): {len(tickers)}")
            log(f"Причины исключения тикеров: {failed_reasons}", level="info")
            return tickers
    except Exception as e:
        log(f"Ошибка получения тикеров: {str(e)}", level="error")
        return []

async def fetch_ohlcv_bybit(symbol, timeframe='1m', limit=200):
    interval_map = {'1m': '1', '5m': '5', '15m': '15', '1h': '60'}  # Bybit интервалы: 1, 5, 15, 60 и т.д.
    interval = interval_map.get(timeframe, '1')
    url = f"{BYBIT_API}/kline"
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        response_text = await resp.text()
                        log(f"Попытка {attempt+1}: Ошибка получения OHLCV для {symbol}: HTTP {resp.status}, Ответ: {response_text}", level="error")
                        if attempt == 2:
                            return pd.DataFrame()
                        await asyncio.sleep(1)
                        continue
                    data = await resp.json()
                    if not data or 'result' not in data or not data['result']['list']:
                        log(f"{symbol} - данные OHLCV пусты. HTTP {resp.status}", level="warning")
                        return pd.DataFrame()
                    # Bybit данные: [[timestamp, open, high, low, close, volume, turnover], ...]
                    df = pd.DataFrame(data['result']['list'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                    return df
        except Exception as e:
            log(f"Попытка {attempt+1}: Ошибка получения OHLCV для {symbol}: {str(e)}", level="error")
            if attempt == 2:
                return pd.DataFrame()
            await asyncio.sleep(1)
