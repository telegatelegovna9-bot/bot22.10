import logging
from logging.handlers import RotatingFileHandler
import sys
import io

# Настройка логгера
logger = logging.getLogger("TradingBot")
logger.setLevel(logging.INFO)

# Ротация логов: максимум 5 МБ, хранить 5 файлов
handler = RotatingFileHandler("bot.log", maxBytes=5_000_000, backupCount=5, encoding='utf-8')
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Вывод в консоль с поддержкой UTF-8
console_handler = logging.StreamHandler(stream=io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

def log(msg, level="info"):
    """Логирование сообщений с указанным уровнем"""
    if level == "error":
        logger.error(msg)
    elif level == "warning":
        logger.warning(msg)
    else:
        logger.info(msg)