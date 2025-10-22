from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from monitor.logger import log
from monitor.settings import load_config, save_config, parse_human_number, human_readable_number

def update_config(key, value):
    """Обновляет конфигурацию и сохраняет её в файл."""
    config = load_config()
    config[key] = value
    save_config(config)
    log(f"Конфигурация обновлена: {key} = {value}", level="info")
    return config

async def start(update: Update, context):
    config = load_config()
    buttons = [
        [KeyboardButton("📴 Выключить бота"), KeyboardButton("📡 Включить бота")],
        [KeyboardButton("📊 Изменить таймфрейм"), KeyboardButton("📈 Изменить порог цены")],
        [KeyboardButton("💹 Изменить фильтр объёма"), KeyboardButton("🛠️ Сбросить настройки")],
        [KeyboardButton("⚙️ Управление индикаторами"), KeyboardButton("🔑 Управление обязательными")],
        [KeyboardButton("📏 Мин. индикаторов")]
    ]
    reply_markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    required_count = len(config.get('required_indicators', []))
    min_ind = config.get('min_indicators', 1)
    await update.message.reply_text(
        f"🚀 Бот активен: {config['bot_status']}\n"
        f"Таймфрейм: {config['timeframe']}\n"
        f"Порог цены: {config['price_change_threshold']}%\n"
        f"Фильтр объёма: {human_readable_number(config['volume_filter'])} USDT\n"
        f"Индикаторы: {sum(config['indicators_enabled'].values())}/{len(config['indicators_enabled'])} включено\n"
        f"Мин. индикаторов: {min_ind}\n"
        f"Обязательные: {required_count}/{len(config['indicators_enabled'])}\n\n"
        "Выберите действие:",
        reply_markup=reply_markup
    )

async def test_telegram(update: Update, context):
    await update.message.reply_text("✅ Тест: Бот работает!")

async def indicators(update: Update, context):
    config = load_config()
    keyboard = []
    for ind, enabled in config['indicators_enabled'].items():
        status = "✅" if enabled else "❌"
        keyboard.append([InlineKeyboardButton(f"{status} {ind}", callback_data=f"toggle_{ind}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Управление индикаторами:", reply_markup=reply_markup)

async def required_indicators(update: Update, context):
    config = load_config()
    keyboard = []
    for ind in config['indicators_enabled']:
        status = "🔑" if ind in config['required_indicators'] else ""
        keyboard.append([InlineKeyboardButton(f"{status} {ind}", callback_data=f"required_{ind}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Управление обязательными индикаторами:", reply_markup=reply_markup)

async def toggle_indicator(update: Update, context):
    query = update.callback_query
    data = query.data
    config = load_config()
    if data.startswith("toggle_"):
        ind = data.replace("toggle_", "")
        config['indicators_enabled'][ind] = not config['indicators_enabled'].get(ind, False)
        save_config(config)
        await query.answer(f"Индикатор {ind} {'включён' if config['indicators_enabled'][ind] else 'выключен'}")
    elif data.startswith("required_"):
        ind = data.replace("required_", "")
        required = config['required_indicators']
        if ind in required:
            required.remove(ind)
        else:
            required.append(ind)
        save_config(config)
        await query.answer(f"Индикатор {ind} {'теперь обязателен' if ind in required else 'не обязателен'}")
    await query.edit_message_text(text="Обновлено!")

async def handle_message(update: Update, context):
    text = update.message.text
    if 'awaiting' in context.user_data:
        key = context.user_data['awaiting']
        try:
            if key == 'timeframe':
                if text not in ['1m', '5m', '15m', '1h']:
                    raise ValueError("Таймфрейм должен быть 1m, 5m, 15m или 1h")
                update_config('timeframe', text)
            elif key == 'volume_filter':
                value = parse_human_number(text)
                update_config('volume_filter', value)
            elif key == 'price_change_threshold':
                value = float(text)
                if value < 0:
                    raise ValueError("Порог цены должен быть положительным")
                update_config('price_change_threshold', value)
            elif key == 'min_indicators':
                value = int(text)
                if value < 1:
                    raise ValueError("Минимальное количество индикаторов должно быть >= 1")
                update_config('min_indicators', value)
            await update.message.reply_text(f"{key} обновлено: {text}")
            context.user_data.pop('awaiting')
        except ValueError as e:
            await update.message.reply_text(f"Ошибка: {str(e)}")
        return

    if text == "📴 Выключить бота":
        update_config('bot_status', False)
        await update.message.reply_text("📴 Бот выключен")
    elif text == "📡 Включить бота":
        update_config('bot_status', True)
        await update.message.reply_text("📡 Бот включен")
    elif text == "🛠️ Сбросить настройки":
        default_config = {
            'timeframe': '1m',
            'volume_filter': 5000000.0,
            'price_change_threshold': 0.5,
            'bot_status': True,
            'indicators_enabled': {
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
            },
            'min_indicators': 1,
            'required_indicators': []
        }
        save_config(default_config)
        await update.message.reply_text("🛠️ Настройки сброшены")
    elif text == "📊 Изменить таймфрейм":
        context.user_data['awaiting'] = 'timeframe'
        await update.message.reply_text("Введите таймфрейм (1m, 5m, 15m, 1h):")
    elif text == "💹 Изменить фильтр объёма":
        context.user_data['awaiting'] = 'volume_filter'
        await update.message.reply_text("Введите минимальный объём (например, 5M, 100K):")
    elif text == "📈 Изменить порог цены":
        context.user_data['awaiting'] = 'price_change_threshold'
        await update.message.reply_text("Введите порог изменения цены в % (например, 0.5):")
    elif text == "⚙️ Управление индикаторами":
        await indicators(update, context)
    elif text == "🔑 Управление обязательными":
        await required_indicators(update, context)
    elif text == "📏 Мин. индикаторов":
        context.user_data['awaiting'] = 'min_indicators'
        await update.message.reply_text("Введите минимальное количество индикаторов (целое число, от 1):")
