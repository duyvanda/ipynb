# -*- coding: utf-8 -*-
import logging
import os
import sys


log_level = os.environ.get("LOG_LEVEL", "INFO")

FORMATTER = logging.Formatter(
    "%(asctime)s — %(name)s:%(lineno)d — %(levelname)s — %(message)s"
)


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.getLevelName(log_level))
    logger.addHandler(get_console_handler())
    logger.propagate = False
    return logger
