import logging
from typing import Any

class ExtendedLogger(logging.Logger):
    def log_multiple(self, level: int, messages: list[str], *args: Any, **kwargs: dict[str, Any]) -> None:
        for message in messages:
            self._log(level, message, args, **kwargs) # type: ignore

logging.setLoggerClass(ExtendedLogger)

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)

logger: ExtendedLogger = logging.getLogger("CEX")

__all__ = ["logger"]