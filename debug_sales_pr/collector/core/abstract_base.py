import abc
import functools
import datetime
import traceback

import pandas as pd
from collector.libs.logger import get_logger
from collector.notify_utils.telegram_utils import TelegramUtils

logger = get_logger(__name__)


def deep_get(dictionary, *keys):
    return functools.reduce(
        lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
        keys,
        dictionary,
    )


def deep_get_with_default(dct, lst_keys, default=None, require=False):
    v = deep_get(dct, *lst_keys)
    if v is None:
        if require:
            if default is None:
                raise KeyError(
                    f"key not found, {dct}, {lst_keys}, default {default}, require {require}"
                )
        return default
    return v


class AbstractBase(abc.ABC):
    def __init__(self, config: dict):
        self.config = config
        self.process_type = self.get_process_info(["process_type"])
        self.process_group = self.get_process_info(["process_group"])
        self.process_name = self.get_process_info(["process_name"])
        self.execution_date = self.get_process_info(["execution_date"])
        telegram_conf = self.get_param_config(["telegram"])
        self.telegram_utils = TelegramUtils(telegram_conf)
        self.df_telegram_users = pd.DataFrame(self.get_param_config(["telegram_users"]))
        self.process_info = "process_type={0} process_group={1} process_name={2}".format(
            self.process_type,
            self.process_group,
            self.process_name,
            self.execution_date,
        )

    def log_telegram(self, log_message, log_level="INFO"):
        for user_id in self.df_telegram_users["id"]:
            self.telegram_utils.send_message(user_id, "```{}```".format(log_message))
        {"ERROR": logger.error, "INFO": logger.info, "WARNING": logger.warning,}.get(
            log_level.upper(), logger.info
        )(log_message)

    def log_error_telegram(self):
        msg_error = self.process_info + "\n" + traceback.format_exc()
        for user_id in self.df_telegram_users["id"]:
            self.telegram_utils.send_message(user_id, "```{}```".format(msg_error))
        logger.error(msg_error)

    @abc.abstractmethod
    def execute(self, *args):
        pass

    def get_param_config(self, keys: list, require=True):
        try:
            default_value = deep_get(self.config, *keys)
            _value = deep_get_with_default(
                self.config["app"]["params"], keys, default_value, require
            )
            logger.info("load param key " + str(keys) + ": " + str(_value))
            return _value
        except KeyError:
            msg_error = self.process_info + " not found param key " + str(keys)
            logger.error(msg_error)
            for user_id in self.df_telegram_users["id"]:
                self.telegram_utils.send_message(user_id, "```{}```".format(msg_error))
            raise KeyError(msg_error)

    def get_process_info(self, keys):
        _value = deep_get(self.config["app"], *keys)
        if _value is None:
            msg_error = self.process_info + " not found process key " + str(keys)
            logger.error(msg_error)
            for user_id in self.df_telegram_users["id"]:
                self.telegram_utils.send_message(user_id, "```{}```".format(msg_error))
            raise KeyError(msg_error)
        else:
            logger.info("load process key " + str(keys) + ": " + str(_value))
            return _value

    def wrapper_simple_log(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            started_at = datetime.datetime.now()
            message = "{0} start at: {1}".format(
                self.process_info, started_at.strftime("%Y-%m-%d %H:%M:%S")
            )
            logger.info(message)
            try:
                func(self, *args, **kwargs)
            except Exception as e:
                msg_error = self.process_info + "\n" + traceback.format_exc()
                for user_id in self.df_telegram_users["id"]:
                    self.telegram_utils.send_message(
                        user_id, "```{}```".format(msg_error)
                    )
                logger.error(msg_error)
                raise e
            finished_at = datetime.datetime.now()
            message = "{0} stop at: {1}".format(
                self.process_info, finished_at.strftime("%Y-%m-%d %H:%M:%S")
            )
            logger.info(message)
            message = "{0} duration time: {1}".format(
                self.process_info, finished_at - started_at
            )
            logger.info(message)

        return wrap

    wrapper_simple_log = staticmethod(wrapper_simple_log)
