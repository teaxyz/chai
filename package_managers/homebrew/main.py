import os
import tempfile
from datetime import datetime
from typing import Optional

from git import Repo

from core.config import Config, PackageManager
from core.db import DB
from core.logger import Logger
from core.models import Package

logger = Logger("homebrew_commits")


class HomebrewManager:
    def __init__(self, db: DB, config: Config):
        self.db = db
        self.config = config
        self._setup_source(config)

    def _setup_source(self, config: Config):
        """Setup the Homebrew source in the database"""
        self.source = config.pm_config.source
        self.package_manager = config.pm_config.pm_id

    def _get_github_url(self, package: Package) -> Optional[str]:
        """Get GitHub repository URL for a package if it exists"""
        repo_type = self.db.get_homepage_url_type()
        if not repo_type:
            logger.warn("No homepage URL type found")
            return None

        url = self.db.get_github_url_for_package(package.id, repo_type.id)
        if not url:
            logger.warn(f"No GitHub URL found for package {package.name}")
            return None

        # Convert HTTP URLs to git URLs for cloning
        if url.startswith("http"):
            url = url.replace("https://github.com/", "git@github.com:")
            if not url.endswith(".git"):
                url += ".git"

        return url

    def process_commit_graph(self, package: Package, repo_path: str):
        """Process the commit graph for a package"""
        repo = Repo(repo_path)

        # Process all commits
        for commit in repo.iter_commits():
            # Get or create author and committer
            author_user, _ = self.db.get_or_create_user(
                commit.author.name, commit.author.email, self.source.id
            )
            committer_user, _ = self.db.get_or_create_user(
                commit.committer.name, commit.committer.email, self.source.id
            )

            # Process signature if exists
            signature = None
            if commit.gpgsig:
                key_id = (
                    commit.gpgsig.key_id
                    if hasattr(commit.gpgsig, "key_id")
                    else "unknown"
                )
                verification_status = "valid" if commit.gpgsig.verify() else "invalid"
                signature, _ = self.db.get_or_create_signature(
                    key_id, verification_status, author_user.id
                )

            # Create commit
            commit_data = {
                "package_id": package.id,
                "sha": commit.hexsha,
                "author_id": author_user.id,
                "committer_id": committer_user.id,
                "message": commit.message,
                "committed_date": datetime.fromtimestamp(commit.committed_date),
                "authored_date": datetime.fromtimestamp(commit.authored_date),
                "signature_id": signature.id if signature else None,
            }
            db_commit = self.db.create_commit(commit_data)

            # Process parents
            for parent in commit.parents:
                parent_commit = self.db.get_commit_by_package_and_sha(
                    package.id, parent.hexsha
                )
                if parent_commit:
                    self.db.create_commit_parent(db_commit.id, parent_commit.id)

    def process_package(self, package: Package):
        """Process a Homebrew package"""
        github_url = self._get_github_url(package)
        if not github_url:
            logger.log(f"No GitHub URL found for package {package.name}")
            return

        # Clone repo to temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = os.path.join(tmpdir, package.name)
            try:
                repo = Repo.clone_from(github_url, repo_path)
                self.process_commit_graph(package, repo_path)
            except Exception as e:
                logger.error(
                    f"Error processing repository for {package.name}: {str(e)}"
                )

    def process_all_packages(self):
        """Process all Homebrew packages"""
        packages = self.db.select_packages_by_package_manager_id(self.package_manager)

        total = len(packages)
        logger.log(f"Found {total} packages to process")

        for i, package in enumerate(packages, 1):
            try:
                logger.log(f"Processing package {i}/{total}: {package.name}")
                self.process_package(package)
            except Exception as e:
                logger.error(f"Error processing package {package.name}: {str(e)}")
                continue


if __name__ == "__main__":
    db = DB()
    config = Config(PackageManager.HOMEBREW, db)
    manager = HomebrewManager(db, config)
    manager.process_all_packages()
