import os
import tarfile
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any

from requests import get
from core.logger import Logger


@dataclass
class Data:
    file_path: str
    file_name: str
    content: Any  # json or bytes


class Fetcher:
    def __init__(self, name: str, source: str):
        self.name = name
        self.source = source
        self.output = f"data/{name}"
        self.logger = Logger(f"{name}_fetcher")

    def write(self, files: list[Data]):
        """generic write function for some collection of files"""

        # prep the file location
        now = datetime.now().strftime("%Y-%m-%d")
        root_path = f"{self.output}/{now}"

        # write
        # it can be anything - json, tarball, etc.
        for item in files:
            file_path = item.file_path
            file_name = item.file_name
            file_content = item.content
            full_path = os.path.join(root_path, file_path)

            # make sure the path exists
            os.makedirs(full_path, exist_ok=True)

            with open(os.path.join(full_path, file_name), "wb") as f:
                self.logger.debug(f"writing {full_path}")
                f.write(file_content)

        # update the latest symlink
        self.update_symlink(now)

    def update_symlink(self, latest_path: str):
        latest_symlink = f"{self.output}/latest"
        if os.path.islink(latest_symlink):
            self.logger.debug(f"removing existing symlink {latest_symlink}")
            os.remove(latest_symlink)

        self.logger.debug(f"creating symlink {latest_symlink} -> {latest_path}")
        os.symlink(latest_path, latest_symlink)

    def fetch(self):
        response = get(self.source)
        try:
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"error fetching {self.source}: {e}")
            raise e

        return response.content


class TarballFetcher(Fetcher):
    def __init__(self, name: str, source: str):
        super().__init__(name, source)

    def fetch(self) -> list[Data]:
        content = super().fetch()

        bytes_io_object = BytesIO(content)
        bytes_io_object.seek(0)

        files = []
        with tarfile.open(fileobj=bytes_io_object, mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.isfile():
                    bytes_io_file = BytesIO(tar.extractfile(member).read())
                    destination_key = member.name
                    file_name = destination_key.split("/")[-1]
                    file_path = "/".join(destination_key.split("/")[:-1])
                    self.logger.debug(f"file_path/file_name: {file_path}/{file_name}")
                    files.append(Data(file_path, file_name, bytes_io_file.read()))

        return files


class JSONFetcher(Fetcher):
    def __init__(self, name: str, source: str):
        super().__init__(name, source)

    def fetch(self):
        pass


class YAMLFetcher(Fetcher):
    def __init__(self, name: str, source: str):
        super().__init__(name, source)

    def fetch(self):
        pass
