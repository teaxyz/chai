#!/usr/bin/env python3

import json
import os
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from typing import List, Optional

import requests
from core.logger import Logger

logger = Logger("pypi_fetcher")

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

def get_all_packages() -> List[str]:
    """Fetch list of all packages from PyPI simple index."""
    url = "https://pypi.org/simple/"
    response = requests.get(url)
    response.raise_for_status()
    
    parser = SimpleIndexParser()
    parser.feed(response.text)
    return parser.packages

def get_package_data(session: requests.Session, package_name: str) -> Optional[dict]:
    """Fetch JSON data for a specific package."""
    url = f"https://pypi.org/pypi/{package_name}/json"
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching {package_name}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON for {package_name}: {e}")
        return None

def main():
    logger.log("\n🚀 Starting PyPI package data fetcher...")
    
    # Create session for connection pooling
    session = requests.Session()
    
    # Get all package names
    logger.log("📋 Fetching package list from PyPI...")
    packages = get_all_packages()
    logger.log(f"✅ Found {len(packages)} packages")

    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_dir = os.path.join("data", "pypi", timestamp)
    os.makedirs(output_dir, exist_ok=True)
    logger.log(f"📂 Created output directory: {output_dir}")

    # Save package list for reference
    package_list_file = os.path.join(output_dir, "package_list.json")
    with open(package_list_file, 'w') as f:
        json.dump(packages, f)
    logger.log(f"💾 Saved package list to {package_list_file}")

    # Process packages in batches
    batch_size = 1000
    current_batch = []
    batch_num = 1
    failed_packages = []

    # Optional: limit packages for testing
    if "--test" in sys.argv:
        packages = packages[:10]
        logger.log("🧪 Test mode: limiting to 10 packages")

    logger.log("\n🔄 Starting package data download...")
    start_time = time.time()
    
    for i, package_name in enumerate(packages, 1):
        # Fetch package data
        logger.log(f"📦 Fetching {package_name} ({i}/{len(packages)})", end='\r')
        package_data = get_package_data(session, package_name)
        
        if package_data:
            current_batch.append(package_data)
        else:
            failed_packages.append(package_name)

        # Save batch if it reaches batch_size or is the last package
        if len(current_batch) >= batch_size or i == len(packages):
            if current_batch:  # Only save if we have data
                batch_file = os.path.join(output_dir, f"packages_batch_{batch_num}.json")
                with open(batch_file, 'w') as f:
                    json.dump(current_batch, f)
                logger.log(f"\n💾 Saved batch {batch_num} ({len(current_batch)} packages)")
                current_batch = []
                batch_num += 1

        # Save failed packages periodically
        if failed_packages and (len(failed_packages) % 100 == 0 or i == len(packages)):
            failed_file = os.path.join(output_dir, "failed_packages.json")
            with open(failed_file, 'w') as f:
                json.dump(failed_packages, f)
            logger.log(f"\n⚠️  Updated failed packages list ({len(failed_packages)} failures)")

        # Rate limiting
        time.sleep(1)  # 1 second between requests

        # Progress logging
        if i % 100 == 0:
            elapsed_time = time.time() - start_time
            avg_time_per_package = elapsed_time / i
            remaining_packages = len(packages) - i
            estimated_remaining_time = remaining_packages * avg_time_per_package
            
            logger.log(f"\n📊 Progress update:")
            logger.log(f"   - Processed: {i}/{len(packages)} packages ({i/len(packages)*100:.1f}%)")
            logger.log(f"   - Elapsed time: {elapsed_time/60:.1f} minutes")
            logger.log(f"   - Estimated remaining time: {estimated_remaining_time/60:.1f} minutes")
            logger.log(f"   - Failed packages: {len(failed_packages)}")

    # Final stats
    total_time = time.time() - start_time
    logger.log("\n✨ Download completed!")
    logger.log(f"📊 Final statistics:")
    logger.log(f"   - Total packages processed: {len(packages)}")
    logger.log(f"   - Successful batches: {batch_num - 1}")
    logger.log(f"   - Failed packages: {len(failed_packages)}")
    logger.log(f"   - Total time: {total_time/60:.1f} minutes")
    logger.log(f"   - Average time per package: {total_time/len(packages):.2f} seconds")
    
    if failed_packages:
        logger.log(f"\n⚠️  Some packages failed to download. See {output_dir}/failed_packages.json")
    
    logger.log("\n💡 Next steps:")
    logger.log("1. Run the PyPI pipeline to process this data:")
    logger.log("   docker compose up pypi")

if __name__ == "__main__":
    main()
