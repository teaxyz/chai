import json
import glob
import os
from typing import Dict, Generator, List, Any
from pathlib import Path

from core.config import URLTypes, UserTypes, PMConf
from core.transformer import Transformer
from core.utils import safe_int
from core.db import DB
from core.logger import Logger
from core.models import URL, PackageURL

class PyPITransformer(Transformer):
    """Transform PyPI package data into CHAI's package format."""

    def __init__(self, url_types: URLTypes, user_types: UserTypes, pm_config: PMConf, db: DB, data_dir: str = "/data/pypi"):
        """Initialize PyPI transformer."""
        super().__init__("pypi")
        self.url_types = url_types
        self.user_types = user_types
        self.pm_config = pm_config
        self.db = db
        self.data_dir = data_dir
        self.logger = Logger("pypi_transformer")
        self.test_mode = False
        
        # Load progress to know how many batches to process
        self.progress_file = os.path.join(data_dir, "progress.json")
        try:
            with open(self.progress_file, 'r') as f:
                self.progress = json.load(f)
                self.total_batches = self.progress["batch_num"]
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error loading progress.json: {e}")
            self.progress = None
            # Count JSON files in directory
            json_files = glob.glob(os.path.join(data_dir, "[0-9]*.json"))
            self.total_batches = len(json_files)

    def _read_batch_files(self) -> Generator[Dict, None, None]:
        """
        Process package data from JSON files in data_dir.
        Files are named 1.json, 2.json, etc.
        Yields each package's data.
        """
        total_packages = 0
        self.logger.log(f"Processing {self.total_batches} batch files")
        
        processed_count = 0
        for batch_num in range(1, self.total_batches + 1):
            file_path = os.path.join(self.data_dir, f"{batch_num}.json")
            try:
                with open(file_path, 'r') as f:
                    packages = json.load(f)
                    self.logger.log(f"Processing batch {batch_num} with {len(packages)} packages")
                    for package in packages:
                        if self.test_mode and processed_count >= 100:
                            self.logger.log("Test mode: Processed 100 entries, stopping")
                            return
                        yield package
                        processed_count += 1
                        total_packages += 1
            except (FileNotFoundError, json.JSONDecodeError) as e:
                self.logger.error(f"Error processing {file_path}: {e}")
                continue
        
        self.logger.log(f"Total packages processed by transformer: {total_packages}")

    def packages(self) -> Generator[Dict[str, Any], None, None]:
        """Transform PyPI package data into CHAI's package format."""
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info")
                if not info:
                    continue
                
                name = info.get("name")
                if not name:
                    continue
                
                package_data = {
                    "derived_id": f"pypi/{name}",
                    "name": name,
                    "package_manager_id": self.pm_config.pm_id,
                    "import_id": f"pypi/{name}",
                    "readme": info.get("description", "")
                }
                
                # Update package cache immediately
                self.db.package_cache[f"pypi/{name}"] = package_data
                
                yield package_data
            except Exception as e:
                self.logger.error(f"Error processing package: {e}")
                continue

    def urls(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI URLs into CHAI's URL format."""
        seen_urls = set()
        
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info", {})
                
                # Extract URLs from various fields
                urls = []
                
                # Homepage URL
                homepage = info.get("home_page")
                if homepage and isinstance(homepage, str):
                    homepage = homepage.strip()
                    if homepage:
                        urls.append((homepage, self.url_types.homepage))
                
                # Documentation URL
                docs_url = info.get("docs_url")
                if docs_url and isinstance(docs_url, str):
                    docs_url = docs_url.strip()
                    if docs_url:
                        urls.append((docs_url, self.url_types.documentation))
                
                # Project URLs
                project_urls = info.get("project_urls", {})
                if project_urls and isinstance(project_urls, dict):
                    for label, url in project_urls.items():
                        if not url or not isinstance(url, str):
                            continue
                            
                        url = url.strip()
                        if not url:
                            continue
                            
                        # Map common project URL labels to URL types
                        url_type = None
                        label_lower = label.lower()
                        if "source" in label_lower or "repository" in label_lower or "github" in label_lower:
                            url_type = self.url_types.repository
                        elif "doc" in label_lower:
                            url_type = self.url_types.documentation
                        elif "home" in label_lower:
                            url_type = self.url_types.homepage
                        
                        if url_type:
                            urls.append((url, url_type))
                
                # Deduplicate and yield URLs
                for url, url_type in urls:
                    if url not in seen_urls:
                        seen_urls.add(url)
                        yield {
                            "url": url,
                            "url_type_id": url_type
                        }
                        
            except Exception as e:
                self.logger.error(f"Error processing URLs: {e}")
                continue

    def package_urls(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI package URLs into CHAI's package URL format."""
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info", {})
                name = info.get("name")
                if not name:
                    continue
                
                # Get package from cache
                package = self.db.package_cache.get(f"pypi/{name}")
                if not package:
                    continue
                    
                package_id = package.get("id")
                if not package_id:
                    continue
                
                # Extract URLs from various fields
                urls = []
                
                # Homepage URL
                homepage = info.get("home_page")
                if homepage and isinstance(homepage, str):
                    homepage = homepage.strip()
                    if homepage:
                        # Get URL from cache or create new one
                        url_obj = self.db.url_cache.get(homepage)
                        if url_obj:
                            yield {
                                "package_id": package_id,
                                "url_id": url_obj["id"]
                            }
                
                # Documentation URL
                docs_url = info.get("docs_url")
                if docs_url and isinstance(docs_url, str):
                    docs_url = docs_url.strip()
                    if docs_url:
                        # Get URL from cache or create new one
                        url_obj = self.db.url_cache.get(docs_url)
                        if url_obj:
                            yield {
                                "package_id": package_id,
                                "url_id": url_obj["id"]
                            }
                
                # Project URLs
                project_urls = info.get("project_urls", {})
                if project_urls and isinstance(project_urls, dict):
                    for label, url in project_urls.items():
                        if not url or not isinstance(url, str):
                            continue
                            
                        url = url.strip()
                        if not url:
                            continue
                            
                        # Map common project URL labels to URL types
                        url_type = None
                        label_lower = label.lower()
                        if "source" in label_lower or "repository" in label_lower or "github" in label_lower:
                            url_type = self.url_types.repository
                        elif "doc" in label_lower:
                            url_type = self.url_types.documentation
                        elif "home" in label_lower:
                            url_type = self.url_types.homepage
                        
                        if url_type:
                            # Get URL from cache or create new one
                            url_obj = self.db.url_cache.get(url)
                            if url_obj:
                                yield {
                                    "package_id": package_id,
                                    "url_id": url_obj["id"]
                                }
                    
            except Exception as e:
                self.logger.error(f"Error processing package URLs: {e}")
                continue

    def versions(self) -> Generator[Dict[str, Any], None, None]:
        """Transform PyPI versions into CHAI's format."""
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info")
                if not info:
                    continue
                    
                name = info.get("name")
                version = info.get("version")
                if not name or not version:
                    continue
                
                # Get package from cache
                package = self.db.package_cache.get(f"pypi/{name}")
                if not package:
                    self.logger.warn(f"Package not found in cache: pypi/{name}")
                    continue
                    
                package_id = package.get("id")
                if not package_id:
                    self.logger.warn(f"Package has no ID in cache: pypi/{name}")
                    continue
                
                # Get license
                license_name = info.get("license") or "Unknown"
                license_id = self.db.select_license_by_name(license_name, create=True)
                if not license_id:
                    continue
                
                # Get downloads - use last_month if available, otherwise 0
                downloads = info.get("downloads", {})
                if not isinstance(downloads, dict):
                    downloads = {}
                download_count = downloads.get("last_month", 0)
                if download_count < 0:  # PyPI uses -1 for unknown
                    download_count = 0
                
                # Get release info
                urls = package_data.get("urls", [])
                if urls and isinstance(urls, list):
                    release = urls[0]  # Use first file for metadata
                    size = release.get("size", 0)
                    published_at = release.get("upload_time_iso_8601")
                    checksum = release.get("digests", {}).get("sha256", "")
                else:
                    size = 0
                    published_at = info.get("created")
                    checksum = ""
                
                # Build version data
                yield {
                    "import_id": f"pypi/{name}/{version}",
                    "package_id": package_id,
                    "version": version,
                    "size": size or 0,
                    "published_at": published_at,
                    "license_id": license_id,
                    "downloads": download_count,
                    "checksum": checksum,
                    "yanked": info.get("yanked", False)
                }
                    
            except Exception as e:
                self.logger.error(f"Error processing version: {e}")
                continue

    def dependencies(self) -> Generator[Dict[str, Any], None, None]:
        """Transform PyPI dependency data into CHAI's dependency format."""
        runtime_type = self.db.select_dependency_type_by_name("runtime", create=True)
        if not runtime_type:
            self.logger.error("Failed to get/create runtime dependency type")
            return iter(())
        
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info")
                if not info:
                    continue
                    
                name = info.get("name")
                version = info.get("version")
                if not name or not version:
                    continue
                
                # Get version ID
                version_obj = self.db.select_version_by_import_id(f"pypi/{name}/{version}")
                if not version_obj:
                    continue
                
                # Process dependencies
                requires_dist = info.get("requires_dist", [])
                if requires_dist is None:
                    requires_dist = []
                    
                for req in requires_dist:
                    try:
                        if not req:
                            continue
                            
                        # Parse dependency name and version
                        dep_name, dep_version = self._parse_dependency(req)
                        if not dep_name:
                            continue
                            
                        # Get dependent package
                        dep_package = self.db.select_package_by_import_id(f"pypi/{dep_name}")
                        if not dep_package:
                            continue
                            
                        yield {
                            "version_id": version_obj.id,
                            "package_id": dep_package.id,
                            "dependency_type_id": runtime_type.id,
                            "version_range": dep_version,
                            "optional": False  # PyPI doesn't have optional dependencies
                        }
                    except Exception as e:
                        self.logger.error(f"Error processing dependency {req}: {e}")
                        continue
            except Exception as e:
                self.logger.error(f"Error processing dependencies: {e}")
                continue

    def users(self) -> Generator[Dict[str, Any], None, None]:
        """Skip user data as we can't get GitHub info from PyPI API."""
        return iter(())

    def user_packages(self) -> Generator[Dict[str, Any], None, None]:
        """Skip user-package relationships as we can't get GitHub info from PyPI API."""
        return iter(())

    def user_versions(self) -> Generator[Dict[str, Any], None, None]:
        """Skip user-version relationships as we can't get GitHub info from PyPI API."""
        return iter(())

    def _parse_dependency(self, req: str) -> tuple[str, str]:
        """Parse a dependency string into name and version range.
        
        Examples:
            "charset-normalizer (>=2,<4)" -> ("charset-normalizer", ">=2,<4")
            "idna (>=2.5,<4)" -> ("idna", ">=2.5,<4")
            "urllib3 (>=1.21.1,<3)" -> ("urllib3", ">=1.21.1,<3")
            "PySocks (>=1.5.6,!=1.5.7)" -> ("PySocks", ">=1.5.6,!=1.5.7")
        """
        if "; " in req:  # Remove any extra conditions
            req = req.split("; ")[0]
        
        if "(" in req and ")" in req:
            name = req.split(" (")[0].strip()
            version = req.split("(")[1].rstrip(")").strip()
            version = version.replace(" ", "")  # Clean up version string
            return name, version
        return req.strip(), ""  # No version constraint
