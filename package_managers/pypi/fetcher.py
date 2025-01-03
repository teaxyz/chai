import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from typing import Generator, List, Optional
import multiprocessing

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
        self.data_dir = "/data/pypi"
        self.process_file = os.path.join(self.data_dir, "progress.json")
        self.packages_file = os.path.join(self.data_dir, "packages.txt")
        
    def _save_process(self, batch_num: int, downloaded: int, fetched: int, total: int):
        """Save current process to progress.json"""
        process = {
            "batch_num": batch_num,
            "downloaded": downloaded,  # successful downloads
            "fetched": fetched,      # total attempted downloads
            "total": total,
            "timestamp": datetime.now().isoformat()
        }
        with open(self.process_file, 'w') as f:
            json.dump(process, f, indent=2)
    
    def _load_process(self) -> tuple[int, int, int, int]:
        """Load process from progress.json if exists"""
        try:
            with open(self.process_file, 'r') as f:
                process = json.load(f)
                return (
                    process["batch_num"],
                    process.get("downloaded", 0),
                    process.get("fetched", 0),
                    process["total"]
                )
        except (FileNotFoundError, json.JSONDecodeError):
            return 0, 0, 0, 0
    
    def _save_package_list(self, packages: List[str]):
        """Save package list to packages.txt"""
        with open(self.packages_file, 'w') as f:
            for package in packages:
                f.write(f"{package}\n")
    
    def _load_package_list(self) -> List[str]:
        """Load package list from packages.txt if exists"""
        try:
            with open(self.packages_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []

    def _get_package_list(self) -> List[str]:
        """Fetch list of all packages from PyPI simple index."""
        url = f"{self.base_url}/simple/"
        response = self.session.get(url)
        response.raise_for_status()
        
        parser = SimpleIndexParser()
        parser.feed(response.text)
        return parser.packages
    
    def _get_package_data(self, package_name: str) -> Optional[dict]:
        """Fetch JSON data for a specific package."""
        # Remove any /simple/ prefix if present
        package_name = package_name.replace('/simple/', '')
        # Remove any trailing slash
        package_name = package_name.rstrip('/')
        url = f"{self.base_url}/pypi/{package_name}/json"
        
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
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Check if we need to resume
        current_batch, downloaded, fetched, total = self._load_process()
        
        # Get package list
        if current_batch == 0 or fetched == total:  # Check fetched instead of downloaded
            # Fresh start or completed before
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
                
                with open(file_path, 'w') as f:
                    json.dump(results, f)
                
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
