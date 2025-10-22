import json
import os
from monitor.logger import log

CONFIG_PATH = "config.json"

def load_config():
    """Загружает конфигурацию из config.json"""
    try:
        if not os.path.exists(CONFIG_PATH):
            log(f"Файл {CONFIG_PATH} не найден, создаётся новый", level="warning")
            default_config = {
                "telegram_token": "",
                "chat_id": "",
                "timeframe": "1m",
                "volume_filter": 5000000.0,
                "price_change_threshold": 0.5,
                "bot_status": True,
                "indicators_enabled": {
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
                "min_indicators": 1,
                "required_indicators": []
            }
            save_config(default_config)
            return default_config
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            log(f"Конфигурация загружена: {config}", level="info")
            return config
    except Exception as e:
        log(f"Ошибка загрузки конфигурации: {str(e)}", level="error")
        raise

def save_config(config):
    """Сохраняет конфигурацию в config.json"""
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        log(f"Конфигурация сохранена в {CONFIG_PATH}", level="info")
    except PermissionError:
        log(f"Ошибка: Нет прав для записи в {CONFIG_PATH}", level="error")
        raise
    except Exception as e:
        log(f"Ошибка сохранения конфигурации: {str(e)}", level="error")
        raise

def parse_human_number(text):
    """Парсит человеко-читаемые числа (5M, 100K)"""
    text = text.lower().strip()
    try:
        if text.endswith('m'):
            return float(text[:-1]) * 1_000_000
        elif text.endswith('k'):
            return float(text[:-1]) * 1_000
        return float(text)
    except ValueError:
        raise ValueError("Неверный формат числа. Используйте число или формат 5M, 100K")

def human_readable_number(number):
    """Форматирует число в человеко-читаемый вид"""
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return str(number)