from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, List, Optional, Any
from html.parser import HTMLParser
from dataclasses import dataclass
from urllib.parse import urljoin
from datetime import datetime
import multiprocessing
import json
import time
import os

import requests

from core.config import Config
from core.fetcher import Data, Fetcher
from core.logger import Logger


class SimpleIndexParser(HTMLParser):
    """Parser for PyPI's simple index page to extract package names."""
    def __init__(self):
        super().__init__()
        self.packages = []

    def handle_starttag(self, tag: str, attrs: List[tuple]):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href":
                    # Package names are the href values
                    self.packages.append(value)


class PyPIFetcher(Fetcher):
    """Custom fetcher for PyPI that handles both simple index and JSON API."""
    
    def __init__(self, name: str, config: Config):
        super().__init__(name, config)
        self.session = requests.Session()
        self.base_url = "https://pypi.org"
        self.rate_limit_delay = 0.1  # seconds between requests
        self.batch_size = 100  # packages per batch
        self.max_workers = multiprocessing.cpu_count() * 4  # Number of threads for parallel downloads
        self.data_dir = "/data/pypi" # We will mount to ./data/pypi on the host
        self.process_file = os.path.join(self.data_dir, "progress.json") # to store the progress of the fetch
        self.packages_file = os.path.join(self.data_dir, "packages.txt") # a list of all package names
        
    def _save_process(self, batch_num: int, downloaded: int, fetched: int, total: int) -> None:
        """Save current process to progress.json"""
        process = {
            "batch_num": batch_num,
            "downloaded": downloaded,  # successful downloads
            "fetched": fetched,      # total attempted downloads
            "total": total,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(self.process_file, 'w') as f:
                json.dump(process, f, indent=2)
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to open process file for writing: {e}")
        except json.JSONEncodeError as e:
            self.logger.error(f"Failed to serialize process data to JSON: {e}")
    
    def _load_process(self) -> tuple[int, int, int, int]:
        """Load process from progress.json if exists"""
        try:
            with open(self.process_file) as f:
                process = json.load(f)
                return (
                    process["batch_num"],
                    process.get("downloaded", 0),
                    process.get("fetched", 0),
                    process["total"]
                )
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Failed to load process file: {e}")
            return 0, 0, 0, 0
    
    def _save_package_list(self, packages: List[str]):
        """Save package list to packages.txt"""
        try:
            with open(self.packages_file, 'w') as f:
                f.writelines(f"{package}\n" for package in packages)
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to open package list file for writing: {e}")
    
    def _load_package_list(self) -> List[str]:
        """Load package list from packages.txt if exists"""
        try:
            with open(self.packages_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to load package list file: {e}")
            return []

    def _get_package_list(self) -> List[str]:
        """Fetch list of all packages from PyPI simple index."""
        url = urljoin(self.base_url, "simple")  # PyPI simple index
        try:
            response = self.session.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch package list from {url}")
            self.logger.error(f"Status code: {response.status_code if 'response' in locals() else 'N/A'}")
            self.logger.error(f"Response body: {response.text if 'response' in locals() else 'N/A'}")
            self.logger.error(f"Error: {str(e)}")
            return []
        
        parser = SimpleIndexParser()
        parser.feed(response.text)
        return parser.packages
    
    def _get_package_data(self, package_name: str) -> dict[str, Any] | None:
        """Fetch JSON data for a specific package."""
        # Remove any /simple/ prefix if present
        package_name = package_name.replace('/simple/', '')
        # Remove any trailing slash
        package_name = package_name.rstrip('/')

        base_api_url = urljoin(self.base_url, "pypi/")
        package_url = urljoin(base_api_url, f"{package_name}/")
        url = urljoin(package_url, "json")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {package_name}: {e}")
            return None
    
    def _download_batch(self, packages: List[str]) -> List[dict]:
        """Download a batch of packages in parallel."""
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_package = {
                executor.submit(self._get_package_data, package): package 
                for package in packages
            }
            for future in as_completed(future_to_package):
                package = future_to_package[future]
                try:
                    data = future.result()
                    if data:
                        results.append(data)
                except Exception as e:
                    self.logger.error(f"Error downloading {package}: {e}")
        return results

    def fetch(self) -> Generator[Data, None, None]:
        """
        Fetch package data from PyPI:
        1. Check if we need to resume or start fresh
        2. Get/load list of all packages
        3. Split packages into batches
        4. Download each batch in parallel
        5. Save raw data to /data/pypi/[batch_num].json in container
        6. Track progress in progress.json
        """
        # Create data directory if needed
        try:
            os.makedirs(self.data_dir, exist_ok=True)
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to create data directory: {e}")
            return
        
        # Check if we need to resume
        current_batch, downloaded, fetched, total = self._load_process()
        
        # If everything is fetched, don't do anything
        # TODO: implement a re-download logic
        if fetched == total and total > 0:
            self.logger.log(f"All packages already fetched ({downloaded}/{fetched}/{total})")
            return
        
        # Get package list
        if current_batch == 0:  # Only get new list if fresh start
            self.logger.log("Starting fresh download")
            packages = self._get_package_list()
            self._save_package_list(packages)
            downloaded = 0
            fetched = 0
        else:
            # Resume from previous run
            self.logger.log(f"Resuming from batch {current_batch} ({downloaded}/{fetched}/{total} packages)")
            packages = self._load_package_list()
            if not packages:
                self.logger.log("No saved package list found, starting fresh")
                packages = self._get_package_list()
                self._save_package_list(packages)
                downloaded = 0
                fetched = 0
        
        total_packages = len(packages)
        self.logger.log(f"Found {total_packages} packages")
        
        # Skip already downloaded batches
        start_idx = current_batch * self.batch_size
        packages = packages[start_idx:]
        
        # Process remaining packages in batches
        for i in range(0, len(packages), self.batch_size):
            batch = packages[i:i + self.batch_size]
            batch_num = (start_idx + i)//self.batch_size + 1
            total_batches = (total_packages + self.batch_size - 1)//self.batch_size
            self.logger.log(f"Downloading batch {batch_num}/{total_batches}")
            
            # Download batch in parallel
            results = self._download_batch(batch)
            
            # Update counters
            fetched = start_idx + i + len(batch)  # Count all attempted packages
            downloaded += len(results) if results else 0  # Count only successful downloads
            
            if results:
                # Save batch to JSON file
                file_name = f"{batch_num}.json"
                file_path = os.path.join(self.data_dir, file_name)
                
                try:
                    with open(file_path, 'w') as f:
                        json.dump(results, f)
                except (IOError, OSError) as e:
                    self.logger.error(f"Failed to open batch file for writing: {e}")
                except json.JSONEncodeError as e:
                    self.logger.error(f"Failed to serialize batch data to JSON: {e}")
                
                # Update progress
                self._save_process(batch_num, downloaded, fetched, total_packages)
                
                # Yield Data object for tracking
                yield Data(
                    file_path=self.data_dir,
                    file_name=file_name,
                    content=json.dumps(results).encode('utf-8')
                )
            else:
                # Still save progress even if no results
                self._save_process(batch_num, downloaded, fetched, total_packages)
            
            # Rate limiting between batches
            time.sleep(self.rate_limit_delay)
