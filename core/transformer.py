import csv
import os
from typing import Dict

from sqlalchemy import UUID

from core.logger import Logger

# this is a temporary fix, but sometimes the raw files have weird characters
# and lots of data within certain fields
# this fix allows us to read the files with no hassles
csv.field_size_limit(10000000)


# the transformer class knows what files to open, and provide a generic wrapper
# for the data within the files
# each package manager will have its own transformer, that knows what data needs to be
# extracted for our data model
class Transformer:
    def __init__(self, name: str):
        self.name = name
        self.input = f"data/{name}/latest"
        self.logger = Logger(f"{name}_transformer")
        self.files: Dict[str, str] = {
            "projects": "",
            "versions": "",
            "dependencies": "",
            "users": "",
            "urls": "",
        }
        self.url_types: Dict[str, UUID] = {}

    def finder(self, file_name: str) -> str:
        input_dir = os.path.realpath(self.input)

        for root, _, files in os.walk(input_dir):
            if file_name in files:
                return os.path.join(root, file_name)
        else:
            self.logger.error(f"{file_name} not found in {input_dir}")
            raise FileNotFoundError(f"Missing {file_name} file")

    def packages(self):
        pass

    def versions(self):
        pass

    def dependencies(self):
        pass
