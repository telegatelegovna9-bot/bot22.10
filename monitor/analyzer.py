import pandas as pd
import numpy as np
import talib
from monitor.logger import log

def analyze(df, config, symbol="Unknown"):
    """
    Анализирует свечи и возвращает сигнал (памп/дамп) + инфо.
    Возвращает: (bool is_signal, dict info)
    """
    info = {}
    if len(df) < 50:
        info['debug'] = f"Внимание: для анализа {symbol} доступно только {len(df)} свечей (менее 50)"
        return False, info
    elif len(df) < 200:
        info['debug'] = f"Внимание: для анализа {symbol} доступно {len(df)} свечей (менее 200, требуется для обычных монет)"

    df = df.copy() if any(config.get('indicators_enabled', {}).values()) else df
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)

    indicators = config.get('indicators_enabled', {
        "price_change": True,
        "rsi": True,
        "macd": True,
        "volume_surge": True,
        "bollinger": True,
        "adx": True,
        "rsi_macd_divergence": True,
        "candle_patterns": True,
        "volume_pre_surge": True,
        "ema_crossover": True,
        "obv": True
    })

    # Инициализация переменных
    rsi = np.nan
    macd = np.nan
    macd_cross = False
    macd_bear = False
    sma20 = np.nan
    upper = np.nan
    lower = np.nan
    vol_surge = np.nan
    adx = np.nan
    bullish_divergence = False
    bearish_divergence = False
    bullish_candle = False
    bearish_candle = False
    volume_pre_surge = False
    ema_cross_up = False
    ema_cross_down = False
    obv_trend = np.nan
    obv_rising = False
    obv_falling = False

    # RSI (14)
    if indicators.get('rsi', True) or indicators.get('rsi_macd_divergence', True):
        try:
            df['rsi'] = talib.RSI(df['close'], timeperiod=14)
            rsi = df['rsi'].iloc[-1]
        except Exception as e:
            log(f"Ошибка расчёта RSI для {symbol}: {e}", level="error")

    # MACD
    if indicators.get('macd', True) or indicators.get('rsi_macd_divergence', True):
        try:
            df['macd'], df['signal'], df['macd_hist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            macd = df['macd'].iloc[-1]
            macd_prev = df['macd'].iloc[-2]
            signal = df['signal'].iloc[-1]
            signal_prev = df['signal'].iloc[-2]
            macd_cross = (macd > signal) and (macd_prev <= signal_prev)
            macd_bear = (macd < signal) and (macd_prev >= signal_prev)
        except Exception as e:
            log(f"Ошибка расчёта MACD для {symbol}: {e}", level="error")

    # Bollinger Bands
    if indicators.get('bollinger', True):
        try:
            df['upper'], df['sma20'], df['lower'] = talib.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            sma20 = df['sma20'].iloc[-1]
            upper = df['upper'].iloc[-1]
            lower = df['lower'].iloc[-1]
            info['bollinger'] = 'upper' if df['close'].iloc[-1] > upper else 'lower' if df['close'].iloc[-1] < lower else 'inside'
        except Exception as e:
            log(f"Ошибка расчёта Bollinger Bands для {symbol}: {e}", level="error")

    # Volume Surge
    if indicators.get('volume_surge', True):
        try:
            vol_avg = df['volume'].rolling(window=20).mean().iloc[-1]
            vol_surge = df['volume'].iloc[-1] / vol_avg if vol_avg != 0 else np.nan
            info['volume_surge'] = vol_surge
        except Exception as e:
            log(f"Ошибка расчёта Volume Surge для {symbol}: {e}", level="error")

    # ADX
    if indicators.get('adx', True):
        try:
            df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
            adx = df['adx'].iloc[-1]
        except Exception as e:
            log(f"Ошибка расчёта ADX для {symbol}: {e}", level="error")

    # RSI-MACD Divergence
    if indicators.get('rsi_macd_divergence', True):
        try:
            last_close = df['close'].iloc[-1]
            prev_close = df['close'].iloc[-2]
            last_rsi = rsi
            prev_rsi = df['rsi'].iloc[-2]
            last_macd = macd
            prev_macd = df['macd'].iloc[-2]
            bullish_divergence = (last_close < prev_close) and (last_rsi > prev_rsi) and (last_macd > prev_macd)
            bearish_divergence = (last_close > prev_close) and (last_rsi < prev_rsi) and (last_macd < prev_macd)
            info['rsi_macd_divergence'] = 'bullish' if bullish_divergence else 'bearish' if bearish_divergence else 'none'
        except Exception as e:
            log(f"Ошибка расчёта дивергенции для {symbol}: {e}", level="error")

    # Candle Patterns
    if indicators.get('candle_patterns', True):
        try:
            bullish_candle = talib.CDLHAMMER(df['open'], df['high'], df['low'], df['close']).iloc[-1] > 0
            bearish_candle = talib.CDLSHOOTINGSTAR(df['open'], df['high'], df['low'], df['close']).iloc[-1] > 0
            info['bullish_candle'] = bullish_candle
            info['bearish_candle'] = bearish_candle
        except Exception as e:
            log(f"Ошибка расчёта свечных паттернов для {symbol}: {e}", level="error")

    # Volume Pre-Surge
    if indicators.get('volume_pre_surge', True):
        try:
            vol_change = (df['volume'].iloc[-2] - df['volume'].iloc[-3]) / df['volume'].iloc[-3] if df['volume'].iloc[-3] != 0 else 0
            volume_pre_surge = 0.2 <= vol_change <= 0.5
            info['volume_pre_surge'] = volume_pre_surge
        except Exception as e:
            log(f"Ошибка расчёта Volume Pre-Surge для {symbol}: {e}", level="error")

    # EMA Crossover
    if indicators.get('ema_crossover', True):
        try:
            df['ema12'] = talib.EMA(df['close'], timeperiod=12)
            df['ema26'] = talib.EMA(df['close'], timeperiod=26)
            ema_cross_up = (df['ema12'].iloc[-1] > df['ema26'].iloc[-1]) and (df['ema12'].iloc[-2] <= df['ema26'].iloc[-2])
            ema_cross_down = (df['ema12'].iloc[-1] < df['ema26'].iloc[-1]) and (df['ema12'].iloc[-2] >= df['ema26'].iloc[-2])
            info['ema_cross_up'] = ema_cross_up
            info['ema_cross_down'] = ema_cross_down
        except Exception as e:
            log(f"Ошибка расчёта EMA Crossover для {symbol}: {e}", level="error")

    # OBV
    if indicators.get('obv', True):
        try:
            df['obv'] = talib.OBV(df['close'], df['volume'])
            obv_trend = df['obv'].iloc[-1] - df['obv'].iloc[-2]
            obv_rising = obv_trend > 0
            obv_falling = obv_trend < 0
            info['obv_trend'] = obv_trend
        except Exception as e:
            log(f"Ошибка расчёта OBV для {symbol}: {e}", level="error")

    # Подсчёт сработавших индикаторов
    triggered = []
    if indicators.get('rsi', True) and not pd.isna(rsi):
        if rsi > 70 or rsi < 30: triggered.append('rsi')
    if indicators.get('macd', True) and (macd_cross or macd_bear): triggered.append('macd')
    if indicators.get('volume_surge', True) and not pd.isna(vol_surge) and vol_surge > 2: triggered.append('volume_surge')
    if indicators.get('bollinger', True) and info.get('bollinger') != 'inside': triggered.append('bollinger')
    if indicators.get('adx', True) and not pd.isna(adx) and adx > 25: triggered.append('adx')
    if indicators.get('rsi_macd_divergence', True) and info.get('rsi_macd_divergence') != 'none': triggered.append('rsi_macd_divergence')
    if indicators.get('candle_patterns', True) and (bullish_candle or bearish_candle): triggered.append('candle_patterns')
    if indicators.get('volume_pre_surge', True) and volume_pre_surge: triggered.append('volume_pre_surge')
    if indicators.get('ema_crossover', True) and (ema_cross_up or ema_cross_down): triggered.append('ema_crossover')
    if indicators.get('obv', True) and (obv_rising or obv_falling): triggered.append('obv')

    count_triggered = len(triggered)
    total_indicators = sum(indicators.values())
    info['count_triggered'] = count_triggered
    info['total_indicators'] = total_indicators

    # Проверка минимального количества и обязательных индикаторов
    required = config.get('required_indicators', [])
    min_ind = config.get('min_indicators', 1)
    all_required = all(r in triggered for r in required)
    is_signal = all_required and count_triggered >= min_ind

    # Определение типа сигнала (pump/dump)
    signal_type = ""
    if is_signal:
        price_change = (df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100
        if price_change > config['price_change_threshold']:
            signal_type = "pump"
        elif price_change < -config['price_change_threshold']:
            signal_type = "dump"
    info['type'] = signal_type

    # Комментарий
    comment_parts = []
    if indicators.get('rsi', True):
        comment_parts.append(f"RSI={rsi:.1f}" if not pd.isna(rsi) else "RSI=NaN")
    if indicators.get('macd', True):
        comment_parts.append(f"MACD={'бычий' if macd_cross else 'медвежий' if macd_bear else 'нейтральный'}")
    if indicators.get('volume_surge', True):
        comment_parts.append(f"объём x{vol_surge:.2f}" if not pd.isna(vol_surge) else "объём=NaN")
    if indicators.get('adx', True):
        comment_parts.append(f"ADX={adx:.1f}" if not pd.isna(adx) else "ADX=NaN")
    if indicators.get('rsi_macd_divergence', True):
        comment_parts.append(f"Дивергенция={'бычья' if bullish_divergence else 'медвежья' if bearish_divergence else 'нет'}")
    if indicators.get('candle_patterns', True):
        comment_parts.append(f"Свечной паттерн={'Hammer' if bullish_candle else 'Shooting Star' if bearish_candle else 'нет'}")
    if indicators.get('volume_pre_surge', True):
        comment_parts.append(f"Рост объёма={'да' if volume_pre_surge else 'нет'}")
    if indicators.get('ema_crossover', True):
        comment_parts.append(f"EMA Crossover={'бычий' if ema_cross_up else 'медвежий' if ema_cross_down else 'нет'}")
    if indicators.get('obv', True):
        comment_parts.append(f"OBV={'растёт' if obv_rising else 'падает' if obv_falling else 'стабилен'}")
    info["comment"] = ", ".join(comment_parts) if comment_parts else "Нет активных индикаторов"

    # Детали для логов
    if not signal_type:
        if 'debug' not in info:
            info['debug'] = f"Нет сигнала для {symbol}"
    else:
        info['debug'] = f"Сигнал сгенерирован для {symbol}: {signal_type}, сработало {count_triggered} из {total_indicators}"

    return bool(signal_type), info