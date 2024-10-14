from os import getenv
import time
import sys
import traceback

DEBUG = getenv("DEBUG", "false").lower() == "true"

# use inspect to print the line of code as well?
# caller = inspect.currentframe().f_back
# filename = caller.f_code.co_filename, lineno = caller.f_lineno


def as_minutes(seconds: float) -> float:
    return seconds / 60


class Logger:
    SILENT = 0
    NORMAL = 1
    VERBOSE = 2

    def __init__(self, name: str, mode=NORMAL, start=time.time()) -> None:
        self.name = name
        self.start = start
        self.mode = Logger.VERBOSE if DEBUG else mode

    def print(self, msg: str):
        print(f"{self.time_diff():.2f}: [{self.name}]: {msg}")

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
