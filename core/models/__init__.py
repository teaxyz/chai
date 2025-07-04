# __init__.py
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, declarative_base, relationship

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=naming_convention)


class BaseModel:
    # we have UUIDs, strings, datetimes, ints, and floats
    def to_dict_v2(self) -> dict[str, str | UUID | datetime | int | float]:
        """Return a dictionary of all non-None attributes."""
        return {
            attr: getattr(self, attr)
            for attr in self.__table__.columns.keys()  # noqa: SIM118
            if getattr(self, attr) is not None
        }


Base = declarative_base(metadata=metadata, cls=BaseModel)


class Package(Base):
    __tablename__ = "packages"
    __table_args__ = (
        UniqueConstraint(
            "package_manager_id", "import_id", name="uq_package_manager_import_id"
        ),
    )

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    derived_id = Column(String, nullable=False, unique=True)  # package_manager/name
    name = Column(String, nullable=False, index=True)
    package_manager_id = Column(
        UUID(as_uuid=True),
        ForeignKey("package_managers.id"),
        nullable=False,
        index=True,
    )
    import_id = Column(String, nullable=False, index=True)
    readme = Column(String, nullable=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    def to_dict(self):
        return {
            "derived_id": self.derived_id,
            "name": self.name,
            "package_manager_id": self.package_manager_id,
            "import_id": self.import_id,
            "readme": self.readme,
        }


class PackageManager(Base):
    __tablename__ = "package_managers"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    source_id = Column(
        UUID(as_uuid=True), ForeignKey("sources.id"), nullable=False, unique=True
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class Version(Base):
    __tablename__ = "versions"
    __table_args__ = (
        UniqueConstraint("package_id", "version", name="uq_package_version"),
    )
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    package_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    version = Column(String, nullable=False, index=True)
    import_id = Column(String, nullable=False, index=True)
    # size, published_at, license_id, downloads, checksum
    # are nullable bc not all sources provide them
    size = Column(Integer, nullable=True, index=True)
    published_at = Column(DateTime, nullable=True, index=True)
    license_id = Column(
        UUID(as_uuid=True), ForeignKey("licenses.id"), nullable=True, index=True
    )
    downloads = Column(Integer, nullable=True, index=True)
    checksum = Column(String, nullable=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    package: Mapped[Package] = relationship()
    license: Mapped[License] = relationship()

    def to_dict(self):
        return {
            "package_id": self.package_id,
            "version": self.version,
            "import_id": self.import_id,
            "size": self.size,
            "published_at": self.published_at,
            "license_id": self.license_id,
            "downloads": self.downloads,
            "checksum": self.checksum,
        }


class License(Base):
    __tablename__ = "licenses"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    name = Column(String, nullable=False, unique=True, index=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class DependsOn(Base):
    __tablename__ = "dependencies"
    __table_args__ = (
        UniqueConstraint(
            "version_id",
            "dependency_id",
            "dependency_type_id",
            name="uq_version_dependency_type",
        ),
    )
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    version_id = Column(
        UUID(as_uuid=True), ForeignKey("versions.id"), nullable=False, index=True
    )
    dependency_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    # ideally, these are non-nullable but diff package managers are picky about this
    dependency_type_id = Column(
        UUID(as_uuid=True), ForeignKey("depends_on_types.id"), nullable=True, index=True
    )
    semver_range = Column(String, nullable=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    version: Mapped[Version] = relationship()
    dependency: Mapped[Package] = relationship()
    dependency_type: Mapped[DependsOnType] = relationship()

    def to_dict(self):
        return {
            "version_id": self.version_id,
            "dependency_id": self.dependency_id,
            # "dependency_type_id": self.dependency_type_id,
            "semver_range": self.semver_range,
        }


class DependsOnType(Base):
    __tablename__ = "depends_on_types"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    name = Column(String, nullable=False, unique=True, index=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class LoadHistory(Base):
    __tablename__ = "load_history"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    package_manager_id = Column(
        UUID(as_uuid=True), ForeignKey("package_managers.id"), nullable=False
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


# authoritative source of truth for all our sources
class Source(Base):
    __tablename__ = "sources"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    type = Column(String, nullable=False, unique=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


# this is a collection of all the different type of URLs
class URL(Base):
    __tablename__ = "urls"
    __table_args__ = (UniqueConstraint("url_type_id", "url", name="uq_url_type_url"),)
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    url_trgm_idx = Index(
        "ix_urls_url_trgm",
        "url",
        postgresql_using="gin",
        postgresql_ops={"url": "gin_trgm_ops"},
    )
    url = Column(String, nullable=False)
    url_type_id = Column(
        UUID(as_uuid=True), ForeignKey("url_types.id"), nullable=False, index=True
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    def to_dict(self):
        return {"url": self.url, "url_type_id": self.url_type_id}


# homepage, repository, documentation, etc.
class URLType(Base):
    __tablename__ = "url_types"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    name = Column(String, nullable=False, unique=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("source_id", "import_id", name="uq_source_import_id"),
    )
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    username = Column(String, nullable=False, index=True)
    source_id = Column(
        UUID(as_uuid=True), ForeignKey("sources.id"), nullable=False, index=True
    )
    import_id = Column(String, nullable=False, unique=False, index=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    def to_dict(self):
        return {
            "username": self.username,
            "source_id": self.source_id,
            "import_id": self.import_id,
        }


class UserVersion(Base):
    __tablename__ = "user_versions"
    __table_args__ = (
        UniqueConstraint("user_id", "version_id", name="uq_user_version"),
    )
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    version_id = Column(
        UUID(as_uuid=True), ForeignKey("versions.id"), nullable=False, index=True
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "version_id": self.version_id,
        }


class UserPackage(Base):
    __tablename__ = "user_packages"
    __table_args__ = (
        UniqueConstraint("user_id", "package_id", name="uq_user_package"),
    )
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    package_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "package_id": self.package_id,
        }


class PackageURL(Base):
    __tablename__ = "package_urls"
    __table_args__ = (UniqueConstraint("package_id", "url_id", name="uq_package_url"),)
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    package_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    url_id = Column(
        UUID(as_uuid=True), ForeignKey("urls.id"), nullable=False, index=True
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    # TODO: deprecated
    def to_dict(self):
        return {
            "package_id": self.package_id,
            "url_id": self.url_id,
        }


class LegacyDependency(Base):
    __tablename__ = "legacy_dependencies"
    __table_args__ = (
        UniqueConstraint("package_id", "dependency_id", name="uq_package_dependency"),
    )
    id = Column(Integer, primary_key=True)
    package_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    dependency_id = Column(
        UUID(as_uuid=True), ForeignKey("packages.id"), nullable=False, index=True
    )
    dependency_type_id = Column(
        UUID(as_uuid=True),
        ForeignKey("depends_on_types.id"),
        nullable=False,
        index=True,
    )
    semver_range = Column(String, nullable=True)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class Canon(Base):
    __tablename__ = "canons"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    url_id = Column(
        UUID(as_uuid=True),
        ForeignKey("urls.id"),
        nullable=False,
        index=True,
        unique=True,
    )
    name_trgm_idx = Index(
        "ix_canons_name_trgm",
        "name",
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
    )
    name = Column(String, nullable=False)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )

    url: Mapped[URL] = relationship()


class CanonPackage(Base):
    __tablename__ = "canon_packages"
    id = Column(UUID(as_uuid=True), primary_key=True)
    canon_id = Column(
        UUID(as_uuid=True), ForeignKey("canons.id"), nullable=False, index=True
    )
    package_id = Column(
        UUID(as_uuid=True),
        ForeignKey("packages.id"),
        nullable=False,
        index=True,
        unique=True,
    )
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class TeaRankRun(Base):
    __tablename__ = "tea_rank_runs"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    run = Column(Integer, nullable=False)
    split_ratio = Column(String, nullable=False)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )


class TeaRank(Base):
    __tablename__ = "tea_ranks"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=func.uuid_generate_v4(),
        server_default=func.uuid_generate_v4(),
    )
    tea_rank_run = Column(Integer, nullable=False, index=True)
    canon_id = Column(
        UUID(as_uuid=True), ForeignKey("canons.id"), nullable=False, index=True
    )
    rank = Column(String, nullable=False)
    created_at = Column(
        DateTime, nullable=False, default=func.now(), server_default=func.now()
    )
