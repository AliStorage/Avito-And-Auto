import logging
import sys
from logging.handlers import RotatingFileHandler

# Configure the root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Отключаем избыточные DEBUG-логи asyncio (Proactor и др.)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# Formatter for all handlers
_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Console handler
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.DEBUG)
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)

# File handler with rotation: 5 files of 10 MB each
_file_handler = RotatingFileHandler(
    filename="app.log",
    maxBytes=100 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8"
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Возвращает экземпляр логгера с указанным именем.
    Все логгеры используют одну и ту же конфигурацию и обработчики.
    """
    return logging.getLogger(name)
