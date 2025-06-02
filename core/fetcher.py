import gzip
import json
import os
import tarfile
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from shutil import rmtree
from typing import Any

import git
from requests import get

from core.logger import Logger


@dataclass
class Data:
    file_path: str
    file_name: str
    content: Any  # json or bytes


class Fetcher:
    def __init__(self, name: str, source: str, no_cache: bool, test: bool):
        self.name = name
        self.source = source
        self.output = f"data/{name}"
        self.logger = Logger(f"{name}_fetcher")
        self.no_cache = no_cache
        self.test = test

    def write(self, files: list[Data]):
        """generic write function for some collection of files"""

        # prep the file location
        now = datetime.now().strftime("%Y-%m-%d")
        root_path = f"{self.output}/{now}"

        # write
        # it can be anything - json, tarball, etc.
        for item in files:
            self.logger.debug(f"writing {item.file_path}/{item.file_name}")
            file_path = item.file_path
            file_name = item.file_name
            file_content = item.content
            full_path = os.path.join(root_path, file_path)

            # make sure the path exists
            os.makedirs(full_path, exist_ok=True)

            with open(os.path.join(full_path, file_name), "wb") as f:
                if isinstance(file_content, (list, dict)):
                    # Convert JSON-serializable objects to string
                    file_content = json.dumps(file_content)

                # Ensure content is bytes before writing
                if isinstance(file_content, str):
                    file_content = file_content.encode("utf-8")

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

    def fetch(self) -> bytes:
        if not self.source:
            raise ValueError("source is not set")

        response = get(self.source)
        try:
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"error fetching {self.source}: {e}")
            raise e
        return response.content

    def cleanup(self):
        if self.no_cache:
            # TODO: it's deleting everything here
            rmtree(self.output, ignore_errors=True)
            os.makedirs(self.output, exist_ok=True)


class TarballFetcher(Fetcher):
    def __init__(self, name: str, source: str, no_cache: bool, test: bool):
        super().__init__(name, source, no_cache, test)

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


# GZip compresses only one file, so file_path and file_name are not used
class GZipFetcher(Fetcher):
    def __init__(
        self,
        name: str,
        source: str,
        no_cache: bool,
        test: bool,
        file_path: str,
        file_name: str,
    ):
        super().__init__(name, source, no_cache, test)
        self.file_path = file_path
        self.file_name = file_name

    def fetch(self) -> list[Data]:
        content = super().fetch()
        files = []

        decompressed = gzip.decompress(content).decode("utf-8")
        files.append(Data(self.file_path, self.file_name, decompressed.encode("utf-8")))

        return files


class GitFetcher(Fetcher):
    def __init__(self, name: str, source: str, no_cache: bool, test: bool):
        super().__init__(name, source, no_cache, test)

    def fetch(self) -> str:
        # assume that source is a git repo whose main branch needs to be cloned
        # we'll first prep the output directory, then clone, then update the symlinks
        # NOTE: this is what the main Fetcher does, but slightly modified for this case

        now = datetime.now().strftime("%Y-%m-%d")
        root_dir = f"{self.output}/{now}"
        os.makedirs(root_dir, exist_ok=True)

        # now, clone the repo here
        self.logger.debug(f"Cloning {self.source} into {root_dir}...")
        _ = git.Repo.clone_from(self.source, root_dir, depth=1, branch="main")
        self.logger.debug("Repository cloned successfully.")

        # update the symlinks
        self.update_symlink(now)

        return root_dir
