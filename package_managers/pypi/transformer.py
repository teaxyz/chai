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
from package_managers.pypi.structs import DependencyType


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
        
        for batch_num in range(1, self.total_batches + 1):
            file_path = os.path.join(self.data_dir, f"{batch_num}.json")
            try:
                with open(file_path, 'r') as f:
                    packages = json.load(f)
                    self.logger.log(f"Processing batch {batch_num} with {len(packages)} packages")
                    for package in packages:
                        yield package
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
                
                # yield URLs
                for url, url_type in urls:
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

                import_id = f"pypi/{name}"
                
                # Homepage URL
                homepage = info.get("home_page")
                if homepage and isinstance(homepage, str):
                    homepage = homepage.strip()
                    if homepage:
                        yield {
                            "import_id": import_id,
                            "url": homepage,
                            "url_type_id": self.url_types.homepage
                        }
                
                # Documentation URL
                docs_url = info.get("docs_url")
                if docs_url and isinstance(docs_url, str):
                    docs_url = docs_url.strip()
                    if docs_url:
                        yield {
                            "import_id": import_id,
                            "url": docs_url,
                            "url_type_id": self.url_types.documentation
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
                            yield {
                                "import_id": import_id,
                                "url": url,
                                "url_type_id": url_type
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
                pypi_id = f"pypi/{name}"

                package = self.db.select_package_by_import_id(pypi_id)
                if not package:
                    self.logger.warn(f"Package {pypi_id} not found in database")
                    continue

                # We are reading from .releases, the object key is the version number
                # We need to loop through all the versions from the .releases object
                for version, releases in package_data.get("releases", {}).items():
                    # Check if the release array is empty
                    if len(releases) == 0:
                        continue

                    # As there can be multiple releases for a version, we will take the first one
                    release = releases[0]

                    # Get license
                    license_name = info.get("license") or "Unknown"

                    # Some license has set to a full paragraph
                    # We need to check if the length is greater than 50, and try to extract the name with a comma
                    # And get the first part of that
                    if len(license_name) > 50:
                        license_name = license_name.split(",")[0]
                    
                    # If it still has more than 50 characters, just skip it
                    if len(license_name) > 50:
                        continue

                    # Get downloads
                    downloads = release.get("downloads", -1)

                    # Get size
                    size = release.get("size", 0)

                    # Get published_at
                    published_at = release.get("upload_time_iso_8601", "")

                    # Get checksum
                    checksum = release.get("digests", {}).get("sha256")

                    # Yield data
                    yield {
                        "version": version,
                        "package_id": package.id,
                        "import_id": f"{pypi_id}-{version}",
                        "size": size,
                        "published_at": published_at,
                        "license": license_name,
                        "downloads": downloads,
                        "checksum": checksum,
                    }
                    
            except Exception as e:
                self.logger.error(f"Error processing version: {e}")
                continue

    def dependencies(self) -> Generator[Dict[str, Any], None, None]:
        """Transform PyPI dependency data into CHAI's dependency format."""
        dependency_type = self.db.select_dependency_type_by_name("runtime")
        
        for package_data in self._read_batch_files():
            try:
                info = package_data.get("info")
                if not info:
                    continue
                    
                name = info.get("name")
                if not name:
                    continue
                
                pypi_id = f"pypi/{name}"
                
                # Process dependencies
                requires_dist = info.get("requires_dist", [])
                if requires_dist is None:
                    continue

                version = info.get("version")
                if not version:
                    continue

                for req in requires_dist:
                    try:
                        # Parse dependency name and version
                        dep = self._parse_dependency(req)

                        dep_name = dep[0]
                        dep_version = dep[1]
                        dep_version_range = dep[2]

                        if not dep_name:
                            continue

                        # Sometimes we don't get a version, so we need to get it from the database
                        if not dep_version:
                            # And getting the latest version
                            dep_version_from_db = self.db.select_latest_version_by_import_id(f"pypi/{dep_name}")
                            if dep_version_from_db:
                                dep_version = dep_version_from_db.version
                            else:
                                continue

                        # Use the original version range for semver_range if available
                        # Otherwise use the clean version with an equals operator
                        semver_range = dep_version_range if dep_version_range else f"=={dep_version}"

                        yield {
                            "version_id": f"pypi/{dep_name}-{dep_version}",
                            "import_id": pypi_id,
                            "semver_range": semver_range,
                            "dependency_type_id": dependency_type.id,
                        }
                    except Exception as e:
                        self.logger.error(f"Error processing dependency {req}: {e}")
                        continue
            except Exception as e:
                self.logger.error(f"Error processing package {package_data.get('info', {}).get('name')}: {e}")
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

    def _parse_dependency(self, req: str) -> tuple[str, str, str]:
        """Parse a dependency string into name, clean version, and version range.
        
        Examples:
            "charset-normalizer (>=2,<4)" -> ("charset-normalizer", "2", ">=2,<4")
            "idna (>=2.5,<4)" -> ("idna", "2.5", ">=2.5,<4")
            "urllib3 (>=1.21.1,<3)" -> ("urllib3", "1.21.1", ">=1.21.1,<3")
            "PySocks (>=1.5.6,!=1.5.7)" -> ("PySocks", "1.5.6", ">=1.5.6,!=1.5.7")
            "pycparser ==2.21" -> ("pycparser", "2.21", "==2.21")
            "pytest >=5.4.1,<6.0.0" -> ("pytest", "5.4.1", ">=5.4.1,<6.0.0")
            "python-jose[cryptography] >=3.1.0,<4.0.0" -> ("python-jose", "3.1.0", ">=3.1.0,<4.0.0")
            "GPUtil~=1.4.0" -> ("GPUtil", "1.4.0", "~=1.4.0")
            "requests ~=2.25" -> ("requests", "2.25", "~=2.25")
            "ipykernel<7.0.0,>=6.29.3" -> ("ipykernel", "6.29.3", "<7.0.0,>=6.29.3")
            "pydantic>=2.10" -> ("pydantic", "2.10", ">=2.10")
        """
        try:
            # Remove any extra conditions after semicolon
            if "; " in req:
                req = req.split("; ")[0].strip()
            
            # First, handle extras in square brackets
            name = req
            if "[" in req and "]" in req:
                name = req.split("[")[0].strip()
                after_bracket = req[req.find("]")+1:].strip()
                if after_bracket:
                    req = name + after_bracket
            
            # Handle parentheses format: "name (version)"
            if "(" in req and ")" in req:
                name = req.split(" (")[0].strip()
                version_constraints = req.split("(")[1].rstrip(")").strip()
                # Get both clean version and version range
                clean_version = self._extract_version(version_constraints)
                return name, clean_version, version_constraints.replace(" ", "")
            
            # Handle various version constraint formats
            version_operators = [">=", "<=", "==", "!=", "~=", ">", "<", "="]
            
            # Find the first version operator in the string
            operator_pos = len(req)
            found_operator = None
            
            for op in version_operators:
                pos = req.find(op)
                if pos != -1 and pos < operator_pos:
                    operator_pos = pos
                    found_operator = op
            
            if found_operator:
                # Split at the operator position
                name = req[:operator_pos].strip()
                version_constraints = req[operator_pos:].strip()
                # Get both clean version and version range
                clean_version = self._extract_version(version_constraints)
                return name, clean_version, version_constraints.replace(" ", "")
                
            # If no version info found
            return req.strip(), "", ""
            
        except Exception as e:
            self.logger.error(f"Error parsing dependency string '{req}': {e}")
            return req.strip(), "", ""

    def _extract_version(self, version_constraints: str) -> str:
        """Extract the first valid version number from version constraints.
        
        Examples:
            ">=2,<4" -> "2"
            ">=2.5,<4" -> "2.5"
            ">=1.21.1,<3" -> "1.21.1"
            ">=1.5.6,!=1.5.7" -> "1.5.6"
            "==2.21" -> "2.21"
            ">=5.4.1,<6.0.0" -> "5.4.1"
            ">=3.1.0,<4.0.0" -> "3.1.0"
            "~=1.4.0" -> "1.4.0"
        """
        # Remove all spaces
        version_constraints = "".join(version_constraints.split())
        
        # List of version operators to strip
        operators = [">=", "<=", "==", "!=", "~=", ">", "<", "="]
        
        # Split by comma to handle multiple constraints
        parts = version_constraints.split(",")
        
        for part in parts:
            # Strip any operators
            clean_version = part
            for op in operators:
                if clean_version.startswith(op):
                    clean_version = clean_version[len(op):]
            
            # If we have a valid version number (contains a digit), return it
            if any(c.isdigit() for c in clean_version):
                return clean_version
        
        return ""
