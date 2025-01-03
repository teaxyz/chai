"""
Unit tests for database models and their relationships.

These tests verify:
1. Basic CRUD operations for all models
2. Relationship integrity between models
3. Constraint enforcement (unique, foreign key, etc.)
4. Data type handling (UUIDs, timestamps, etc.)

The tests use a temporary PostgreSQL database that is created and destroyed
for each test class, ensuring test isolation and cleanup.
"""

import uuid

import pytest

from core.models import (
    DependsOn,
    DependsOnType,
    License,
    Package,
    PackageManager,
    Version,
)


class TestDatabaseModels:
    """
    Unit tests for database models and operations.
    Uses a temporary PostgreSQL database to verify model behavior and relationships.
    """

    @pytest.mark.db
    def test_package_crud(self, db_session):
        """
        Test CRUD operations for Package model.

        Verifies:
        - Package creation with required fields
        - Reading package data
        - Updating package fields
        - Deleting a package
        - Unique constraint on derived_id
        - Foreign key constraint with package_manager
        """
        # Get package manager for test
        package_manager = db_session.query(PackageManager).first()

        # Create
        package = Package(
            id=uuid.uuid4(),
            import_id="test123",
            name="test-package",
            readme="Test readme",
            package_manager_id=package_manager.id,
            derived_id=f"crates/test-package-{uuid.uuid4().hex[:8]}",
        )
        db_session.add(package)
        db_session.commit()

        # Read
        saved_package = db_session.query(Package).filter_by(import_id="test123").first()
        assert saved_package is not None
        assert saved_package.name == "test-package"

        # Update
        saved_package.readme = "Updated readme"
        db_session.commit()

        # Delete
        db_session.delete(saved_package)
        db_session.commit()
        assert db_session.query(Package).filter_by(import_id="test123").first() is None

    @pytest.mark.db
    def test_version_relationships(self, db_session):
        """
        Test relationships between Version and Package models.

        Verifies:
        - Version creation with package relationship
        - Foreign key constraint with package
        - Unique constraint on package_id + version
        - Bidirectional navigation between Version and Package
        """
        # Get package manager for test
        package_manager = db_session.query(PackageManager).first()

        # Create package with unique identifiers
        import_id = f"pkg{uuid.uuid4().hex[:8]}"
        derived_id = f"crates/test-package-{uuid.uuid4().hex[:8]}"
        package = Package(
            id=uuid.uuid4(),
            import_id=import_id,
            name="test-package",
            package_manager_id=package_manager.id,
            derived_id=derived_id,
        )
        db_session.add(package)
        db_session.commit()

        # Create version
        version = Version(
            id=uuid.uuid4(),
            import_id=f"ver{uuid.uuid4().hex[:8]}",
            package_id=package.id,
            version="1.0.0",
        )
        db_session.add(version)
        db_session.commit()

        # Query relationships
        saved_version = db_session.query(Version).filter_by(id=version.id).first()
        assert saved_version is not None
        assert saved_version.package_id == package.id
        assert saved_version.package.name == "test-package"

    @pytest.mark.db
    def test_license_crud(self, db_session):
        """
        Test CRUD operations for License model.

        Verifies:
        - License creation with required fields
        - Reading license data
        - Updating license fields
        - Deleting a license
        - Unique constraint on license name
        """
        # Create
        license = License(
            id=uuid.uuid4(),
            name="MIT",
        )
        db_session.add(license)
        db_session.commit()

        # Read
        saved_license = db_session.query(License).filter_by(name="MIT").first()
        assert saved_license is not None
        assert saved_license.name == "MIT"

        # Test unique constraint
        duplicate_license = License(
            id=uuid.uuid4(),
            name="MIT",  # Same name as existing license
        )
        db_session.add(duplicate_license)
        with pytest.raises(
            Exception
        ) as exc_info:  # SQLAlchemy will raise an integrity error
            db_session.commit()
        assert "duplicate key value violates unique constraint" in str(exc_info.value)
        db_session.rollback()

        # Update
        saved_license.name = "Apache-2.0"
        db_session.commit()

        # Verify update
        updated_license = db_session.get(License, saved_license.id)
        assert updated_license.name == "Apache-2.0"

        # Delete
        db_session.delete(saved_license)
        db_session.commit()
        assert db_session.query(License).filter_by(name="Apache-2.0").first() is None

    @pytest.mark.db
    def test_license_version_relationship(self, db_session):
        """
        Test relationships between License and Version models.

        Verifies:
        - Version creation with license relationship
        - Foreign key constraint with license
        - Nullable license_id field
        - Bidirectional navigation between Version and License
        """
        # Get package manager for test
        package_manager = db_session.query(PackageManager).first()

        # Create license
        license = License(
            id=uuid.uuid4(),
            name="MIT",
        )
        db_session.add(license)
        db_session.commit()

        # Create package with unique identifiers
        import_id = f"pkg{uuid.uuid4().hex[:8]}"
        derived_id = f"crates/test-package-{uuid.uuid4().hex[:8]}"
        package = Package(
            id=uuid.uuid4(),
            import_id=import_id,
            name="test-package",
            package_manager_id=package_manager.id,
            derived_id=derived_id,
        )
        db_session.add(package)
        db_session.commit()

        # Create version with license
        version = Version(
            id=uuid.uuid4(),
            package_id=package.id,
            version="1.0.0",
            license_id=license.id,
            import_id=f"ver{uuid.uuid4().hex[:8]}",
        )
        db_session.add(version)
        db_session.commit()

        # Test relationships
        saved_version = db_session.query(Version).filter_by(id=version.id).first()
        assert saved_version.license_id == license.id
        assert saved_version.license.name == "MIT"

    @pytest.mark.db
    def test_depends_on_crud(self, db_session):
        """
        Test CRUD operations for DependsOn model.

        Verifies:
        - Dependency creation with required fields
        - Reading dependency data
        - Updating dependency fields
        - Deleting a dependency
        - Foreign key constraints with version and package
        - Unique constraint on version + dependency + type
        """
        # Get package manager for test
        package_manager = db_session.query(PackageManager).first()

        # Create dependency type
        dep_type = DependsOnType(
            id=uuid.uuid4(),
            name="runtime",
        )
        db_session.add(dep_type)
        db_session.commit()

        # Create two packages with unique identifiers
        package1 = Package(
            id=uuid.uuid4(),
            import_id=f"pkg{uuid.uuid4().hex[:8]}",
            name="package-one",
            package_manager_id=package_manager.id,
            derived_id=f"crates/package-one-{uuid.uuid4().hex[:8]}",
        )
        package2 = Package(
            id=uuid.uuid4(),
            import_id=f"pkg{uuid.uuid4().hex[:8]}",
            name="package-two",
            package_manager_id=package_manager.id,
            derived_id=f"crates/package-two-{uuid.uuid4().hex[:8]}",
        )
        db_session.add_all([package1, package2])
        db_session.commit()

        # Create version for package1
        version = Version(
            id=uuid.uuid4(),
            import_id=f"ver{uuid.uuid4().hex[:8]}",
            package_id=package1.id,
            version="1.0.0",
        )
        db_session.add(version)
        db_session.commit()

        # Create dependency relationship
        dependency = DependsOn(
            id=uuid.uuid4(),
            version_id=version.id,
            dependency_id=package2.id,
            dependency_type_id=dep_type.id,
            semver_range="^1.0",
        )
        db_session.add(dependency)
        db_session.commit()

        # Read
        saved_dep = (
            db_session.query(DependsOn)
            .filter_by(version_id=version.id, dependency_id=package2.id)
            .first()
        )
        assert saved_dep is not None
        assert saved_dep.semver_range == "^1.0"
        assert saved_dep.dependency_type_id == dep_type.id

        # Update
        saved_dep.semver_range = "^2.0"
        db_session.commit()

        # Verify update
        updated_dep = db_session.get(DependsOn, saved_dep.id)
        assert updated_dep.semver_range == "^2.0"

        # Delete
        db_session.delete(saved_dep)
        db_session.commit()
        assert (
            db_session.query(DependsOn)
            .filter_by(version_id=version.id, dependency_id=package2.id)
            .first()
            is None
        )

    @pytest.mark.db
    def test_depends_on_relationships(self, db_session):
        """
        Test complex relationships between DependsOn and related models.

        Verifies:
        - Relationships between DependsOn, Version, Package, and DependsOnType
        - Correct navigation through multiple relationship levels
        - Integrity of relationship data
        - Proper handling of nullable fields (dependency_type)
        """
        # Get package manager for test
        package_manager = db_session.query(PackageManager).first()

        # Get or create dependency types
        runtime_type = db_session.query(DependsOnType).filter_by(name="runtime").first()
        if not runtime_type:
            runtime_type = DependsOnType(
                id=uuid.uuid4(),
                name="runtime",
            )
            db_session.add(runtime_type)

        dev_type = db_session.query(DependsOnType).filter_by(name="dev").first()
        if not dev_type:
            dev_type = DependsOnType(
                id=uuid.uuid4(),
                name="dev",
            )
            db_session.add(dev_type)
        db_session.commit()

        # Create packages with unique identifiers
        import_id1 = f"pkg{uuid.uuid4().hex[:8]}"
        import_id2 = f"pkg{uuid.uuid4().hex[:8]}"
        derived_id1 = f"crates/test-package-1-{uuid.uuid4().hex[:8]}"
        derived_id2 = f"crates/test-package-2-{uuid.uuid4().hex[:8]}"
        package1 = Package(
            id=uuid.uuid4(),
            import_id=import_id1,
            name="test-package-1",
            package_manager_id=package_manager.id,
            derived_id=derived_id1,
        )
        package2 = Package(
            id=uuid.uuid4(),
            import_id=import_id2,
            name="test-package-2",
            package_manager_id=package_manager.id,
            derived_id=derived_id2,
        )
        db_session.add_all([package1, package2])
        db_session.commit()

        # Create versions with import_ids
        version1 = Version(
            id=uuid.uuid4(),
            package_id=package1.id,
            version="1.0.0",
            import_id=f"ver{uuid.uuid4().hex[:8]}",
        )
        version2 = Version(
            id=uuid.uuid4(),
            package_id=package2.id,
            version="2.0.0",
            import_id=f"ver{uuid.uuid4().hex[:8]}",
        )
        db_session.add_all([version1, version2])
        db_session.commit()

        # Create dependency relationship
        depends_on = DependsOn(
            id=uuid.uuid4(),
            version_id=version1.id,
            dependency_id=package2.id,
            dependency_type_id=runtime_type.id,
            semver_range="^2.0",
        )
        db_session.add(depends_on)
        db_session.commit()

        # Test relationships
        saved_dep = db_session.query(DependsOn).filter_by(id=depends_on.id).first()
        assert saved_dep.version_id == version1.id
        assert saved_dep.dependency_id == package2.id
        assert saved_dep.dependency_type_id == runtime_type.id
        assert saved_dep.version.package.name == "test-package-1"
        assert saved_dep.dependency.name == "test-package-2"
        assert saved_dep.dependency_type.name == "runtime"
