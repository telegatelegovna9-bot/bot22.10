# bot.py (corrected version)
import asyncio
import sys
import traceback
import telegram
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from monitor.fetcher import get_all_futures_tickers, fetch_ohlcv_bybit
from monitor.analyzer import analyze
from monitor.logger import log
from monitor.settings import load_config
from monitor.signals import send_signal
from monitor.handlers import start, test_telegram, handle_message, toggle_indicator

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

config = load_config()
scheduler = AsyncIOScheduler(timezone=pytz.UTC)
semaphore = asyncio.Semaphore(25)

EXCLUDED_KEYWORDS = ["ALPHA", "WEB3", "AI", "BOT"]

previous_signals = {}  # Кэш: {symbol: count_triggered}
cached_tickers = {}  # Глобальный кэш для тикеров

async def run_monitor():
    global config, cached_tickers
    config = load_config()
    if not config.get('bot_status', False):
        log("Мониторинг отключен по конфигу.", level="warning")
        return

    try:
        log("Запуск мониторинга...")
        start_time = asyncio.get_event_loop().time()
        if not cached_tickers or cached_tickers.get('timestamp', 0) + 300 < asyncio.get_event_loop().time():
            tickers = await get_all_futures_tickers()
            tickers = [t for t in tickers if not any(k in t.upper() for k in EXCLUDED_KEYWORDS)]
            cached_tickers = {'tickers': tickers, 'timestamp': asyncio.get_event_loop().time()}
            log(f"Получено {len(tickers)} тикеров для обработки (обновлён кэш)", level="info")
        else:
            tickers = cached_tickers['tickers']
            log(f"Получено {len(tickers)} тикеров из кэша", level="info")
        log(f"Всего тикеров после фильтра: {len(tickers)}")

        if not tickers:
            log("Тикеры не найдены, проверка остановлена.", level="warning")
            return

        total, signals = 0, 0

        async def process_symbol(symbol):
            nonlocal total, signals
            async with semaphore:
                await asyncio.sleep(0.1)
                symbol_start_time = asyncio.get_event_loop().time()
                try:
                    log(f"Начало обработки {symbol}", level="info")
                    df = await fetch_ohlcv_bybit(symbol, config['timeframe'])
                    if df.empty:
                        log(f"{symbol} - пустой DataFrame после fetch_ohlcv_bybit", level="warning")
                        return
                    is_signal, info = analyze(df, config, symbol=symbol)
                    total += 1
                    if is_signal:
                        signals += 1
                        count_triggered = info.get('count_triggered', 0)
                        prev_count = previous_signals.get(symbol, 0)
                        if symbol not in previous_signals or count_triggered > prev_count:
                            log(f"Начало отправки сигнала для {symbol}", level="info")
                            await send_signal(symbol, df, info, config)
                            previous_signals[symbol] = count_triggered
                        else:
                            await send_confirmation(symbol, info, config, count_triggered, prev_count)
                    else:
                        log(f"[{symbol}] Нет сигнала. {info.get('debug', 'Нет дополнительной информации')}", level="info")
                    symbol_end_time = asyncio.get_event_loop().time()
                    log(f"Обработка {symbol} завершена за {symbol_end_time - symbol_start_time:.2f} сек", level="debug")
                except Exception as e:
                    log(f"Ошибка обработки {symbol}: {str(e)}", level="error")

        tasks = [process_symbol(symbol) for symbol in tickers]
        await asyncio.gather(*tasks)
        end_time = asyncio.get_event_loop().time()
        log(f"Обработано {total} тикеров, сигналов: {signals}, время обработки: {end_time - start_time:.2f} сек", level="info")
    except Exception as e:
        log(f"Ошибка в run_monitor: {str(e)} | Traceback: {traceback.format_exc()}", level="error")

async def send_confirmation(symbol, info, config, count_triggered, prev_count):
    try:
        bot = telegram.Bot(token=config['telegram_token'])
        signal_type = info.get("type", "")
        if signal_type == "pump":
            icon, label = "🚀", "ПАМП"
        elif signal_type == "dump":
            icon, label = "📉", "ДАМП"
        else:
            icon, label = "⚪", "СИГНАЛ"

        html = (
            f"<b>{icon} Подтверждение {label.lower()}</b>\n"
            f"Монета: <code>{symbol}</code>\n"
            f"Теперь сработало {count_triggered} из {info['total_indicators']} индикаторов (ранее {prev_count})\n"
            f"{info['comment']}"
        )

        await bot.send_message(chat_id=config['chat_id'], text=html, parse_mode="HTML")
        log(f"[{symbol}] Отправлено подтверждение: {label}, сработало {count_triggered}")
    except Exception as e:
        log(f"Ошибка отправки подтверждения для {symbol}: {e}")

async def reload_bot(app):
    """Перезапуск бота через остановку и повторный запуск"""
    global config
    log("Перезагрузка бота...")
    scheduler.remove_all_jobs()
    await app.stop()
    await app.start()
    config = load_config()
    log("Бот перезапущен")

async def main():
    app = ApplicationBuilder().token(config['telegram_token']).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('test', test_telegram))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(toggle_indicator))

    scheduler.add_job(run_monitor, 'interval', seconds=60)
    scheduler.start()
    log("Бот запущен. Используй /start или /test в Telegram.")

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=['message', 'callback_query'])
    await asyncio.Event().wait()  # Keep the loop running

if __name__ == '__main__':
    asyncio.run(main())
