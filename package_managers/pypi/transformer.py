import json
import glob
import os
from typing import Dict, Generator, List, Any

from core.config import URLTypes, UserTypes
from core.transformer import Transformer
from core.utils import safe_int
from package_managers.pypi.structs import DependencyType


class PyPITransformer(Transformer):
    """Transform PyPI package data into CHAI's package format."""

    def __init__(self, url_types: URLTypes, user_types: UserTypes):
        super().__init__("pypi")
        self.url_types = url_types
        self.user_types = user_types
        self.data_dir = "/data/pypi"  # Use absolute path

    def _read_batch_files(self) -> Generator[Dict, None, None]:
        """
        Read all batch files and yield package data.
        Looks for the most recent data directory and processes all batch files.
        """
        # Find the most recent data directory
        if not os.path.exists(self.data_dir):
            self.logger.error(f"❌ Data directory not found: {self.data_dir}")
            self.logger.error("💡 Run fetch_packages.py first to download package data")
            return

        date_dirs = sorted(os.listdir(self.data_dir), reverse=True)
        if not date_dirs:
            self.logger.error("❌ No data directories found in data/pypi/")
            self.logger.error("💡 Run fetch_packages.py first to download package data")
            return

        latest_dir = os.path.join(self.data_dir, date_dirs[0])
        self.logger.log(f"📂 Using data from: {latest_dir}")

        # Process each batch file
        batch_files = sorted(glob.glob(os.path.join(latest_dir, "packages_batch_*.json")))
        if not batch_files:
            self.logger.error("❌ No batch files found in the data directory")
            return

        self.logger.log(f"📦 Found {len(batch_files)} batch files to process")
        total_packages = 0
        
        for i, batch_file in enumerate(batch_files, 1):
            try:
                with open(batch_file, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)
                    batch_size = len(batch_data)
                    total_packages += batch_size
                    self.logger.log(f"🔄 Processing batch {i}/{len(batch_files)} ({batch_size} packages)")
                    for package_data in batch_data:
                        yield package_data
            except (json.JSONDecodeError, FileNotFoundError) as e:
                self.logger.error(f"❌ Error reading {batch_file}: {e}")

        self.logger.log(f"✅ Processed {total_packages} packages from {len(batch_files)} batch files")

    def packages(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI package data into CHAI's package format."""
        for package_data in self._read_batch_files():
            info = package_data.get("info", {})
            if not info:
                self.logger.error(f"Missing info for package: {package_data.get('name', 'UNKNOWN')}")
                continue

            name = info.get("name", "")
            if not name:
                continue

            # Get URLs
            project_urls = info.get("project_urls") or {}
            homepage = info.get("home_page") or project_urls.get("Homepage", "")
            repository = project_urls.get("Source", "")
            documentation = project_urls.get("Documentation", "")

            # Get version and download info
            version = info.get("version", "")
            downloads = info.get("downloads", {})
            download_count = downloads.get("last_month", 0)
            if download_count == -1:  # PyPI returns -1 when stats are disabled
                download_count = 0

            # Get dependencies
            requires_dist = info.get("requires_dist") or []
            dependencies = []
            for req in requires_dist:
                if req:
                    # Parse out extras (e.g., "chardet<6,>=3.0.2; extra == 'use-chardet-on-py3'")
                    if "; extra ==" in req:
                        req = req.split("; extra ==")[0]
                    # Get package name and version constraint
                    parts = req.split(" ", 1)
                    dependencies.append({
                        "name": parts[0],
                        "type": "runtime",
                        "version_constraint": parts[1] if len(parts) > 1 else ""
                    })

            # Get classifiers
            classifiers = info.get("classifiers", [])
            keywords = []
            for classifier in classifiers:
                if classifier.startswith("Topic :: "):
                    keywords.append(classifier.split(" :: ")[-1])

            yield {
                "id": name,
                "name": name,
                "import_id": name,
                "version": version,
                "description": info.get("summary", ""),
                "homepage": homepage,
                "repository": repository,
                "documentation": documentation,
                "license": info.get("license", ""),
                "keywords": keywords,
                "dependencies": dependencies,
                "download_count": download_count,
                "source": "pypi",
                "readme": info.get("description", ""),
                "requires_python": info.get("requires_python", ""),
                "author": info.get("author", ""),
                "author_email": info.get("author_email", ""),
                "maintainer": info.get("maintainer", ""),
                "maintainer_email": info.get("maintainer_email", "")
            }

    def users(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI user data into CHAI's user format."""
        # Process authors and maintainers as users
        seen_users = set()
        for package_data in self._read_batch_files():
            info = package_data.get("info", {})
            if not info:
                continue

            # Process author
            author = info.get("author") or ""
            author_email = info.get("author_email") or ""
            if author and isinstance(author, str):
                author = author.strip()
                if author and author not in seen_users:
                    seen_users.add(author)
                    yield {
                        "username": author,
                        "import_id": author_email.strip() if author_email else author,
                    }

            # Process maintainer
            maintainer = info.get("maintainer") or ""
            maintainer_email = info.get("maintainer_email") or ""
            if maintainer and isinstance(maintainer, str):
                maintainer = maintainer.strip()
                if maintainer and maintainer not in seen_users:
                    seen_users.add(maintainer)
                    yield {
                        "username": maintainer,
                        "import_id": maintainer_email.strip() if maintainer_email else maintainer,
                    }

    def urls(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI URLs into CHAI's URL format."""
        for package_data in self._read_batch_files():
            info = package_data.get("info", {})
            project_urls = info.get("project_urls", {})
            
            # Homepage
            homepage = info.get("home_page")
            if homepage:
                yield {
                    "id": homepage,
                    "url": homepage,
                    "type": self.url_types.homepage,
                }

            # Repository
            repository = project_urls.get("Source")
            if repository:
                yield {
                    "id": repository,
                    "url": repository,
                    "type": self.url_types.repository,
                }

            # Documentation
            docs = project_urls.get("Documentation")
            if docs:
                yield {
                    "id": docs,
                    "url": docs,
                    "type": self.url_types.documentation,
                }

    def versions(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI release data into CHAI's version format."""
        for package_data in self._read_batch_files():
            info = package_data.get("info", {})
            releases = package_data.get("releases", {})
            package_name = info.get("name")
            if not package_name:
                continue

            for version, release_data in releases.items():
                if not release_data:  # Skip empty releases
                    continue
                    
                release = release_data[0]  # Take first release file's data
                yield {
                    "id": f"{package_name}-{version}",
                    "package_id": package_name,
                    "version": version,
                    "created_at": release.get("upload_time", ""),
                    "downloads": safe_int(release.get("downloads", 0)),
                }

    def dependencies(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI dependency data into CHAI's dependency format."""
        for package_data in self._read_batch_files():
            info = package_data.get("info", {})
            package_name = info.get("name")
            if not package_name:
                continue

            requires_dist = info.get("requires_dist", [])
            if requires_dist:
                for req in requires_dist:
                    # Parse requirement string (e.g., "requests>=2.25.1")
                    parts = req.split(";")[0].strip().split(">=")
                    dep_name = parts[0].strip()
                    version = parts[1].strip() if len(parts) > 1 else ""
                    
                    yield {
                        "id": f"{package_name}-{dep_name}",
                        "package_id": package_name,
                        "dependency_id": dep_name,
                        "version_constraint": version,
                        "type": DependencyType.REQUIRES.value,
                    }

            # Python version requirement
            python_requires = info.get("requires_python")
            if python_requires:
                yield {
                    "id": f"{package_name}-python",
                    "package_id": package_name,
                    "dependency_id": "python",
                    "version_constraint": python_requires,
                    "type": DependencyType.REQUIRES_PYTHON.value,
                }

    def user_packages(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI user-package relationships into CHAI's format."""
        # PyPI doesn't provide user-package relationships in the package JSON
        return iter(())

    def user_versions(self) -> Generator[Dict[str, str], None, None]:
        """Transform PyPI user-version relationships into CHAI's format."""
        # PyPI doesn't provide user-version relationships in the package JSON
        return iter(())
