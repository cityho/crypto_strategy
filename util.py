import os
import re
import logging
from enum import Enum
from pathlib import Path
from typing import List, Union, Optional

from dotenv import load_dotenv
from binance.client import Client


_LOGGERS = {}  # cache to avoid duplicate handlers



load_dotenv()


class ENV(Enum):
    API_KEY = os.getenv("API_KEY")
    SECRET_KEY = os.getenv("SECRET_KEY")
    DATA_PATH = os.getenv("DATA_PATH")
    PQ_DATA_PATH = os.getenv("PQ_DATA_PATH")


class BinanceClient:
    def __init__(
            self,
            api_key=ENV.API_KEY.value,
            secret_key=ENV.SECRET_KEY.value,
            **params
    ):
        self.client = Client(
            api_key,
            secret_key
        )


def search_file_list(
    base_path: Union[str, Path],
    pattern: str
) -> List[Path]:
    base_path = Path(base_path)

    if not base_path.exists():
        raise FileNotFoundError(base_path)

    files = base_path.rglob(pattern)

    return sorted(files)


def get_logger(
    name: str = "app",
    level: int = logging.INFO,
    log_file: Optional[str | Path] = None,
) -> logging.Logger:
    """
    Simple shared logger.

    Usage:
        from logger import get_logger
        logger = get_logger(__name__)
        logger.info("hello")

    - No duplicate handlers
    - Console logging by default
    - Optional file logging
    """

    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # prevent double logging via root

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Optional file handler
    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    _LOGGERS[name] = logger
    return logger

if __name__ == "__main__":
    print(BinanceClient().client)