"""
Test Debian package parser functionality.

This module tests the DebianParser class which parses Debian package
and source entries from Packages and Sources files.
"""

import pytest

from package_managers.debian.parser import DebianParser


@pytest.fixture
def simple_package():
    return """Package: 0ad
Version: 0.0.26-1
Installed-Size: 19162
Maintainer: Debian Games Team <pkg-games-devel@lists.alioth.debian.org>
Architecture: amd64
Depends: 0ad-data (>= 0.0.26), 0ad-data-common (>= 0.0.26), libc6 (>= 2.29), libcurl4 (>= 7.16.2), libenet7 (>= 1.3.13), libgloox18, libjsoncpp25 (>= 1.9.5), libminiupnpc17 (>= 1.9.20140610), libnspr4 (>= 2:4.9.2), libnss3 (>= 2:3.22)
Recommends: fonts-freefont-ttf, fonts-texgyre
Suggests: 0ad-dbg
Description: Real-time strategy game of ancient warfare
Homepage: https://play0ad.com/
Section: games
Priority: optional
Filename: pool/main/0/0ad/0ad_0.0.26-1_amd64.deb
Size: 6050744
MD5sum: a777ddf01c18dbdef15c589f8325d7a3
SHA256: 9da19833c1a51e890aa8a11f82ec1e383c0e79410c3d2f6845fd2ec3e23249b8


"""


@pytest.fixture
def simple_source():
    return """Package: 0ad
Binary: 0ad, 0ad-dbg, 0ad-data, 0ad-data-common
Version: 0.0.26-1
Maintainer: Debian Games Team <pkg-games-devel@lists.alioth.debian.org>
Uploaders: Vincent Cheng <vcheng@debian.org>, Euan Kemp <euank@euank.com>
Build-Depends: debhelper-compat (= 13), cmake, dpkg-dev (>= 1.15.5), libboost-dev, libenet-dev (>= 1.3), libopenal-dev, libpng-dev, libsdl2-dev, libtiff5-dev, libvorbis-dev, libxcursor-dev, pkg-config, zlib1g-dev, libcurl4-gnutls-dev, libgloox-dev, libjsoncpp-dev, libminiupnpc-dev, libnspr4-dev, libnss3-dev, libsodium-dev, libwxgtk3.0-gtk3-dev | libwxgtk3.0-dev, python3, python3-dev, libxml2-dev, rust-gdb [amd64 i386 ppc64el]
Architecture: any all
Standards-Version: 4.5.1
Format: 3.0 (quilt)
Files:
 2fc0f38b8a4cf56fea7040fcf5f79ca3 2414 0ad_0.0.26-1.dsc
 35ca57e781448c69ba31323313e972af 31463733 0ad_0.0.26.orig.tar.xz
 f78de44c8a9c32e6be3ae99f2747c330 71948 0ad_0.0.26-1.debian.tar.xz
Vcs-Browser: https://salsa.debian.org/games-team/0ad
Vcs-Git: https://salsa.debian.org/games-team/0ad.git
Directory: pool/main/0/0ad
Priority: optional
Section: games
Testsuite: autopkgtest
Testsuite-Triggers: g++, pyrex


"""


@pytest.fixture
def multiline_binary():
    """Fixture for binary fields, specifically multi-lines ones"""
    return """
Package: binutils
Binary: binutils-for-host, binutils-for-build,
 binutils-ia64-linux-gnu-dbg, binutils-m68k-linux-gnu,
 binutils-mips64el-linux-gnuabin32-dbg, binutils-mipsisa64r6-linux-gnuabin32,
 binutils-mipsisa64r6el-linux-gnuabi64-dbg

"""


@pytest.fixture
def build_depends():
    """Fixture for all kinds of build depends."""
    return """
Package: example
Build-Depends: gcc-11-source (>= 11.3.0-11~), gawk, lib32gcc1-amd64-cross [amd64 arm64 i386 ppc64el x32], g++-11, gm2-11 [!powerpc !ppc64 !x32]
"""


@pytest.mark.parser
class TestDebianParser:
    """Test the Debian parser functionality."""

    def test_build_depends(self, build_depends):
        """Test parsing build depends."""
        parser = DebianParser(build_depends)
        sources = list(parser.parse())
        assert len(sources) == 1
        source = sources[0]
        assert len(source.build_depends) == 5
        assert any(dep.package == "gcc-11-source" for dep in source.build_depends)
        assert any(dep.package == "gawk" for dep in source.build_depends)
        assert any(
            dep.package == "lib32gcc1-amd64-cross" for dep in source.build_depends
        )
        assert any(dep.package == "g++-11" for dep in source.build_depends)
        assert any(dep.package == "gm2-11" for dep in source.build_depends)

    def test_multiline_binary(self, multiline_binary):
        """Test handling of multiline binaries."""
        parser = DebianParser(multiline_binary)
        sources = list(parser.parse())
        assert len(sources) == 1
        source = sources[0]
        assert source.package == "binutils"
        assert source.binary == [
            "binutils-for-host",
            "binutils-for-build",
            "binutils-ia64-linux-gnu-dbg",
            "binutils-m68k-linux-gnu",
            "binutils-mips64el-linux-gnuabin32-dbg",
            "binutils-mipsisa64r6-linux-gnuabin32",
            "binutils-mipsisa64r6el-linux-gnuabi64-dbg",
        ]

    def test_parse_package_data(self, simple_package):
        """Test parsing a typical package entry from Packages file."""
        # Sample package data from a Packages file
        package_data = simple_package

        # Parse the package data
        parser = DebianParser(package_data)
        packages = list(parser.parse())

        # Validate we have one package
        assert len(packages) == 1
        package = packages[0]

        # Test basic fields
        assert package.package == "0ad"
        assert package.version == "0.0.26-1"
        assert package.installed_size == 19162
        assert package.architecture == "amd64"

        # Test maintainer parsing
        assert package.maintainer.name == "Debian Games Team"
        assert package.maintainer.email == "pkg-games-devel@lists.alioth.debian.org"

        # Test dependency parsing
        assert len(package.depends) == 10
        assert package.depends[0].package == "0ad-data"
        assert package.depends[0].semver == ">= 0.0.26"

        # Test recommends parsing
        assert len(package.recommends) == 2
        assert package.recommends[0].package == "fonts-freefont-ttf"

        # Test suggests parsing
        assert len(package.suggests) == 1
        assert package.suggests[0].package == "0ad-dbg"

    def test_parse_source_data(self, simple_source):
        """Test parsing a typical source entry from Sources file."""
        source_data = simple_source
        parser = DebianParser(source_data)
        sources = list(parser.parse())

        # Validate we have one source package
        assert len(sources) == 1
        source = sources[0]

        # Test basic fields
        assert source.package == "0ad"
        assert source.version == "0.0.26-1"

        # Test binary field
        assert isinstance(source.binary, list)  # Fixed: binary should be a list
        assert "0ad" in source.binary
        assert "0ad-dbg" in source.binary
        assert "0ad-data" in source.binary
        assert "0ad-data-common" in source.binary

        # Test maintainer parsing
        assert source.maintainer.name == "Debian Games Team"
        assert source.maintainer.email == "pkg-games-devel@lists.alioth.debian.org"

        # Test uploaders parsing
        assert len(source.uploaders) == 2
        assert source.uploaders[0].name == "Vincent Cheng"
        assert source.uploaders[0].email == "vcheng@debian.org"
        assert source.uploaders[1].name == "Euan Kemp"
        assert source.uploaders[1].email == "euank@euank.com"

        # Test build depends parsing
        assert len(source.build_depends) == 25
        assert any(dep.package == "debhelper-compat" for dep in source.build_depends)

        # Test other source fields
        assert source.format == "3.0 (quilt)"
        assert source.vcs_browser == "salsa.debian.org/games-team/0ad"
        assert source.vcs_git == "salsa.debian.org/games-team/0ad"
        assert source.testsuite == "autopkgtest"
        assert source.testsuite_triggers == "g++, pyrex"
