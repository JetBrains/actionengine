import logging


def get_logger():
    if not hasattr(get_logger, "logger"):
        get_logger.logger = logging.getLogger("actionengine")
    return get_logger.logger


class WithPrefix(logging.LoggerAdapter):
    def __init__(
        self,
        logger: logging.Logger,
        prefix: str = "",
        first_time_prefix: str = None,
    ):
        super(WithPrefix, self).__init__(logger, {})
        self.prefix = prefix
        self.first_time_prefix = first_time_prefix or prefix
        self.first_time = True

    def process(self, msg, kwargs):
        result = (
            "%s %s"
            % (self.first_time_prefix if self.first_time else self.prefix, msg),
            kwargs,
        )

        self.first_time = False
        return result


def get_prefixed_logger(
    logger: logging.Logger, prefix: str, first_time_prefix: str = None
) -> logging.LoggerAdapter:
    """Return a logger with a prefix added to log messages."""
    return WithPrefix(logger, prefix, first_time_prefix=first_time_prefix)


class TextColor:
    @staticmethod
    def yellow(text: str) -> str:
        return f"\x1b[33m{text}\033[0m"

    @staticmethod
    def gray(text: str) -> str:
        return f"\x1b[38;5;242m{text}\x1b[0m"

    @staticmethod
    def grey(text: str) -> str:
        return TextColor.gray(text)

    @staticmethod
    def dimmed_yellow(text: str) -> str:
        return f"\033[2m\033[33m{text}\033[0m"

    @staticmethod
    def green(text: str) -> str:
        return f"\033[92m{text}\033[0m"

    @staticmethod
    def dimmed_green(text: str) -> str:
        return f"\033[2m\033[92m{text}\033[0m"

    @staticmethod
    def blue(text: str) -> str:
        return f"\033[94m{text}\033[0m"

    @staticmethod
    def dimmed_blue(text: str) -> str:
        return f"\033[2m\033[94m{text}\033[0m"

    @staticmethod
    def red(text: str) -> str:
        return f"\033[91m{text}\033[0m"
