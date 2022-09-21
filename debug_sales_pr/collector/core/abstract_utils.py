import abc
import functools

from collector.libs.logger import get_logger

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


class AbstractUtils(abc.ABC):
    def __init__(self, config: dict):
        self.config = config

    @abc.abstractmethod
    def execute(self, *args):
        pass

    def get_param_config(self, keys: list, require=False):
        try:
            default_value = deep_get(self.config, *keys)
            _value = deep_get_with_default(self.config, keys, default_value, require)
            logger.info("load param key " + str(keys) + ": " + str(_value))
            return _value
        except KeyError:
            logger.error("not found param key " + str(keys))
            raise KeyError("not found param key " + str(keys))

    def get_process_info(self, keys):
        _value = deep_get(self.config, *keys)
        if _value is None:
            logger.error("not found process key " + str(keys))
            raise KeyError("not found process key " + str(keys))
        else:
            logger.info("load process key " + str(keys) + ": " + str(_value))
            return _value
