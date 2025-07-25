import sys
import time
import traceback

from core.utils import env_vars

DEBUG = env_vars("DEBUG", "false")


def as_minutes(seconds: float) -> float:
    return seconds / 60


class Logger:
    SILENT = 0
    NORMAL = 1
    VERBOSE = 2

    def __init__(
        self, name: str, mode: int = NORMAL, start: float | None = None
    ) -> None:
        self.name = name
        self.start = start or time.time()
        self.mode = Logger.VERBOSE if DEBUG else mode

    def print(self, msg: str):
        print(f"{self.time_diff():.2f}: [{self.name}]: {msg}", flush=True)

    def error(self, message):
        self.print(f"[ERROR]: {message}")

    def log(self, message):
        if self.mode >= Logger.NORMAL:
            self.print(f"{message}")

    def debug(self, message):
        if self.mode >= Logger.VERBOSE:
            self.print(f"[DEBUG]: {message}")

    def warn(self, message):
        if self.mode >= Logger.NORMAL:
            self.print(f"[WARN]: {message}")

    def is_verbose(self):
        return self.mode >= Logger.VERBOSE

    def time_diff(self):
        return time.time() - self.start

    def exception(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        self.print(f"{exc_type.__name__}: {exc_value}")
        self.print("***** TRACEBACK *****")
        print(f"{''.join(traceback.format_tb(exc_traceback))}")

    def info(self, message):
        self.log(message)

    def warning(self, message):
        self.warn(message)
