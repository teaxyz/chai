import sys
import time
import traceback
from os import getenv

debug = getenv("DEBUG", "false").lower()
DEBUG = debug == "true" or debug == "1"


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
