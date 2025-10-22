import aiohttp
import pandas as pd
from monitor.logger import log
import asyncio

BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"

async def get_all_futures_tickers():
    try:
        url = f"{BINANCE_FAPI}/ticker/24hr"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log(f"Ошибка получения тикеров: HTTP {resp.status}, Ответ: {await resp.text()}", level="error")
                    return []
                data = await resp.json()
                if not data or not isinstance(data, list):
                    log(f"Ошибка: данные тикеров не являются списком или пусты: {data}", level="error")
                    return []
            
            from monitor.settings import load_config
            config = load_config()
            volume_filter = config.get('volume_filter', 5_000_000.0)
            tickers = []
            failed_reasons = {'volume': 0, 'usdt': 0}

            for item in data:
                if not isinstance(item, dict):
                    continue
                symbol = item.get('symbol', '')
                quote_volume = float(item.get('quoteVolume', 0))

                if not symbol.endswith('USDT'):
                    failed_reasons['usdt'] += 1
                    continue
                if quote_volume < volume_filter:
                    failed_reasons['volume'] += 1
                    continue

                tickers.append(symbol)

            log(f"Всего тикеров: {len(data)}, после фильтра по объёму ({volume_filter}): {len(tickers)}")
            log(f"Причины исключения тикеров: {failed_reasons}", level="info")
            return tickers
    except Exception as e:
        log(f"Ошибка получения тикеров: {str(e)}", level="error")
        return []

async def fetch_ohlcv_binance(symbol, timeframe='1m', limit=200):
    interval_map = {'1m': '1m', '5m': '5m', '15m': '1h'}
    interval = interval_map.get(timeframe, '1m')
    url = f"{BINANCE_FAPI}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

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
                    if not data:
                        log(f"{symbol} - данные OHLCV пусты. HTTP {resp.status}", level="warning")
                        return pd.DataFrame()
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                                    'close_time', 'quote_asset_volume', 'num_trades',
                                                    'taker_buy_base', 'taker_buy_quote', 'ignore'])
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