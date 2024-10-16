from os import getenv
import schedule
import time
from threading import Thread
from typing import Callable
from core.logger import Logger

FREQUENCY = int(getenv("FREQUENCY", 24))


class Scheduler:
    def __init__(self, name: str, frequency: int = FREQUENCY):
        self.name = name
        self.frequency = frequency
        self.logger = Logger(f"{name}_scheduler")
        self.job = None
        self.is_running = False

    def start(self, task: Callable, *args):
        self.job = schedule.every(self.frequency).hours.do(task, *args)
        self.is_running = True
        self.logger.log(f"scheduled {self.name} to run every {self.frequency} hours")

        def run_schedule():
            while self.is_running:
                schedule.run_pending()
                time.sleep(1)

        Thread(target=run_schedule, daemon=True).start()

    def stop(self):
        if self.job:
            schedule.cancel_job(self.job)
        self.is_running = False
        self.logger.log(f"stopped {self.name} scheduler")

    def run_now(self, task: Callable, *args):
        self.logger.log(f"running {self.name} now")
        task(*args)
