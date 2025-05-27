# NPM Singleton Package Loader

A utility script for loading a single NPM package and its metadata into the CHAI database.

## Purpose

This script allows you to:

1. Check if an NPM package exists in the CHAI database
2. Fetch package metadata from the NPM registry
3. Verify package URLs (homepage, repository, source)
4. Check dependencies and their existence in CHAI
5. Add the package to the CHAI database

## Usage

1. You must either run this script from the project root directory or specify
   `PYTHONPATH` to point to the root directory, since it imports modules from the `core` library.
2. You must also specify a `CHAI_DATABASE_URL` string

### Method 1: Using pkgx (recommended)

```bash
# Make the script executable
chmod +x scripts/npm-singleton/single.py

# Run with PYTHONPATH set
PYTHONPATH=. scripts/npm-singleton/single.py <package_name> [--dry-run]
```

### Method 2: Using uv directly

```bash
PYTHONPATH=. uv run scripts/npm-singleton/single.py <package_name> [--dry-run]
```

## Arguments

- `package_name`: Name of the NPM package to load (required)
- `--dry-run`: Run in read-only mode without committing to the database

> [!NOTE]
> Strongly recommend running with the `--dry-run` flag first, to see what changes
> you're about to implement. The output looks like:

    ```bash
    ---------------------------------------------
    Package: @types/jest
    ---------------------------------------------
    ✅ @types/jest doesn't exist on CHAI
    ---------------------------------------------
    ✅ OK from NPM
    ---------------------------------------------
    ✅ has homepage: github.com/DefinitelyTyped/DefinitelyTyped
    ✅ has repository: github.com/DefinitelyTyped/DefinitelyTyped.git
    ✅ has source: github.com/DefinitelyTyped/DefinitelyTyped.git
    ---------------------------------------------
    Runtime Dependencies:
    ✅ expect / ^29.0.0 on CHAI
    ✅ pretty-format / ^29.0.0 on CHAI
    ---------------------------------------------
    Dev Dependencies:
    (none)
    ---------------------------------------------
    DRY RUN: Would create the following rows:
    - 1 Package
    - 3 URLs
    - 3 PackageURLs
    - 2 Runtime Dependencies
    - 0 Dev Dependencies
    ---------------------------------------------
    ℹ️ Dry run: No changes committed to database
    ```

> If a dependency doesn't exist on CHAI, you can just run the script for that
> dependency, and then run it for your main package

## Output

The script provides detailed status information about the package:

```
---------------------------------------------
Package: <package_name>
---------------------------------------------
❌ Exiting bc <package_name> exists on CHAI | ✅ <package_name> doesn't exist on CHAI
---------------------------------------------
❌ Exiting bc response error from registry | ✅ OK from NPM
---------------------------------------------
✅ has homepage: <homepage> | ❌ no homepage
✅ has repository: <repository> | ❌ no repository
✅ has source: <source> | ❌ no source
---------------------------------------------
✅ <dependency> / <semver> on CHAI | ❌ <dependency> / <semver> not on CHAI
... for each dependency
---------------------------------------------
```

In dry-run mode, the script will show what changes would be made without committing them to the database.

## Examples

Check a package without adding it to the database:

```bash
PYTHONPATH=. scripts/npm-singleton/single.py react --dry-run
```

Add a package to the database:

```bash
PYTHONPATH=. scripts/npm-singleton/single.py lodash
```

## Tasks

### check

Env: PYTHONPATH=../..
Inputs: PACKAGE

```bash
./single.py $PACKAGE --dry-run
```

### add

Env: PYTHONPATH=../..
Inputs: PACKAGE

```bash
./single.py $PACKAGE
```
