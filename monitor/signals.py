# signals.py (полностью измененный код)
import telegram
from monitor.logger import log
from monitor.charts import create_chart

async def send_signal(symbol, df, info, config):
    try:
        log(f"Начало отправки сигнала для {symbol}")
        bot = telegram.Bot(token=config['telegram_token'])
        last_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        tf_change = (last_close - prev_close) / prev_close * 100 if prev_close != 0 else 0

        signal_type = info.get("type", "")
        count_triggered = info.get("count_triggered", 0)
        total_indicators = info.get("total_indicators", 0)
        count_str = f"Сработало {count_triggered} из {total_indicators} индикаторов"

        if signal_type == "pump":
            icon, label = "🚀", "ПАМП"
        elif signal_type == "dump":
            icon, label = "📉", "ДАМП"
        else:
            icon, label = "⚪", "СИГНАЛ"

        tradingview_url = f"https://www.tradingview.com/chart/?symbol=BYBIT:{symbol.replace('/', '').replace(':', '')}.P"

        html = (
            f"<b>{icon} {label}</b> | <b>{tf_change:.2f}% на момент сигнала</b>\n"
            f"Монета: <code>{symbol}</code>\n"
            f"Цена сейчас: <b>{last_close:.6f} USDT</b>\n"
            f"{count_str}\n"
            f"\nИндикаторы (подтверждение):\n"
        )
        if "rsi" in info:
            html += f"• RSI: <b>{info['rsi']:.1f}</b> (перекупленность/перепроданность)\n"
        if "macd" in info:
            html += f"• MACD: <b>{info['macd']:.6f}</b> (тренд)\n"
        if "volume_surge" in info:
            html += f"• Рост объёма: <b>x{info['volume_surge']:.2f}</b>\n"
        if "bollinger" in info:
            html += f"• Bollinger: <b>{'выше верхней' if info['bollinger'] == 'upper' else 'ниже нижней' if info['bollinger'] == 'lower' else 'внутри'}</b>\n"
        if "adx" in info:
            html += f"• ADX: <b>{info['adx']:.1f}</b> (сила тренда)\n"
        if "rsi_macd_divergence" in info:
            html += f"• Дивергенция: <b>{'бычья' if info['rsi_macd_divergence'] == 'bullish' else 'медвежья' if info['rsi_macd_divergence'] == 'bearish' else 'нет'}</b>\n"
        if "bullish_candle" in info or "bearish_candle" in info:
            candle = "Hammer" if info['bullish_candle'] else "Shooting Star" if info['bearish_candle'] else "нет"
            html += f"• Свечной паттерн: <b>{candle}</b>\n"
        if "volume_pre_surge" in info:
            html += f"• Рост объёма: <b>{'да' if info['volume_pre_surge'] else 'нет'}</b> (20-50%)\n"
        if "ema_cross_up" in info or "ema_cross_down" in info:
            ema_cross = "бычий" if info['ema_cross_up'] else "медвежий" if info['ema_cross_down'] else "нет"
            html += f"• EMA Crossover: <b>{ema_cross}</b> (EMA12/EMA26)\n"
        if "obv_trend" in info:
            obv = "растёт" if info['obv_trend'] > 0 else "падает" if info['obv_trend'] < 0 else "стабилен"
            html += f"• OBV: <b>{obv}</b> (объёмный тренд)\n"
        html += (
            f"\n{info['comment']}\n\n"
            f"<a href=\"{tradingview_url}\">Открыть график на TradingView</a>"
        )

        chart_buf = create_chart(df, symbol, config['timeframe'])
        log(f"Отправка сообщения в чат {config['chat_id']}...")
        if chart_buf is None:
            log(f"График не создан для {symbol}", level="warning")
            await bot.send_message(chat_id=config['chat_id'], text=html + "\n(График недоступен)", parse_mode="HTML")
        else:
            await bot.send_photo(chat_id=config['chat_id'], photo=chart_buf, caption=html, parse_mode="HTML")
        log(f"Сообщение успешно отправлено для {symbol}")
        log(f"[{symbol}] Сигнал отправлен: {label} | {tf_change:.2f}% | {last_close}. Детали: {info['debug']}")
    except Exception as e:
        log(f"Ошибка отправки сигнала для {symbol}: {e}")
        raise
