import logging


class LoggableMixin:
    """简易日志混入类"""

    def __init__(self, name: str = None, console: bool = True, level=logging.INFO):
        self.logger = logging.getLogger(name or self.__class__.__name__)
        if not self.logger.handlers:
            self.logger.setLevel(level)
            if console:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
                self.logger.addHandler(handler)
