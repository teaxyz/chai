# Debian Package Manager Diff Implementation

## Overview
Successfully updated the debian package manager pipeline to follow the same diff structure as pkgx, homebrew, and crates. The new approach follows this pattern:

1. **Fetch data** - Download and extract debian packages and sources files
2. **Parse it** - Parse the debian package/source files into DebianData objects  
3. **Create cache** - Load current state from database into a Cache object
4. **Perform diff** - Compare parsed data against cached data to identify changes
5. **Ingest changes** - Only insert/update/delete what has changed

## Files Created/Modified

### New Files Created

#### `package_managers/debian/db.py`
- New database interface following pkgx pattern
- `DebianDB` class with methods:
  - `set_current_graph()` - Load current packages and dependencies from DB
  - `set_current_urls()` - Load current URLs and package-URL relationships from DB  
  - `ingest()` - Efficiently ingest only the computed diffs

#### `package_managers/debian/diff.py` 
- New diff computation logic following pkgx pattern
- `DebianDiff` class with methods:
  - `diff_pkg()` - Compare package descriptions, return new/updated packages
  - `diff_url()` - Generate and resolve URLs for packages 
  - `diff_pkg_url()` - Compare package-URL relationships
  - `diff_deps()` - Compare dependencies with priority-based deduplication
  - `_generate_chai_urls()` - Generate URLs from debian data (homepage, VCS, archives)
  - `_canonicalize()` - Clean up and normalize URLs

#### `tests/package_managers/debian/test_debian_diff.py`
- Comprehensive test suite modeled after `test_pkgx_diff.py`
- Tests cover all major scenarios:
  - New packages
  - Package description updates  
  - URL changes
  - Dependency changes (add/remove/type changes)
  - Priority-based dependency deduplication
  - Debian-specific dependency types (recommends, suggests)
  - Archive URL generation
  - Missing dependency handling
  - No-change scenarios

### Modified Files

#### `package_managers/debian/main.py`
- **Complete rewrite** to use diff approach
- Now follows the same pattern as pkgx:
  - Fetch data using existing GZipFetcher
  - Parse using DebianParser
  - Load current state into Cache
  - Compute diffs using DebianDiff
  - Ingest only the changes using DebianDB
- Removed dependency on old DebianTransformer and DebianLoader

## Key Features Implemented

### Dependency Priority System
Implements the same priority-based dependency deduplication as other package managers:
- **Runtime > Build > Test** priority order
- If a package depends on the same dependency with multiple types, chooses highest priority
- Handles database constraint on unique (package_id, dependency_id) pairs

### Debian-Specific Features
- **Multiple dependency types**: depends, build_depends, recommends, suggests
- **VCS URL support**: vcs_git, vcs_browser for repository URLs
- **Archive URL generation**: Constructs debian archive URLs from directory + filename
- **Proper handling of build_depends**: Correctly handles build_depends as `list[str]` vs other deps as `list[Depends]`

### URL Generation
Generates multiple URL types from debian data:
- **Homepage URLs** from homepage field
- **Repository URLs** from vcs_git and vcs_browser fields  
- **Source URLs** from directory + filename (debian archive paths)
- **URL canonicalization** with proper protocol handling

### Efficient Change Detection
Only processes actual changes:
- **New packages**: Create with all metadata
- **Description updates**: Update readme field only
- **URL changes**: Add new URLs, link to packages
- **Dependency changes**: Add/remove specific dependencies with proper typing
- **No-change scenarios**: Skip processing entirely

## Testing Coverage

The test suite provides comprehensive coverage:

### Core Scenarios
- ✅ Completely new packages
- ✅ Existing packages with no changes  
- ✅ Package description updates
- ✅ URL updates (old → new)
- ✅ Dependency changes (add/remove/change type)

### Dependency Testing
- ✅ Priority-based deduplication (Runtime > Build > Test)
- ✅ Dependency type changes (runtime ↔ build) 
- ✅ Multiple dependency types on same package
- ✅ Missing dependency handling
- ✅ Debian-specific types (recommends, suggests)

### URL Testing  
- ✅ Homepage URL generation
- ✅ VCS URL generation
- ✅ Archive URL construction
- ✅ URL canonicalization

### Edge Cases
- ✅ Empty/missing package names
- ✅ Missing dependencies in cache
- ✅ No existing URLs vs new URLs
- ✅ Malformed URL handling

## Benefits of New Approach

1. **Performance**: Only processes changes rather than bulk-loading everything
2. **Consistency**: Follows same pattern as other package managers  
3. **Reliability**: Comprehensive test coverage ensures correctness
4. **Maintainability**: Clear separation of concerns (fetch → parse → diff → ingest)
5. **Efficiency**: Minimizes database operations and memory usage

## Migration Notes

The new implementation is a **complete replacement** of the old approach:
- ❌ **Removed**: `DebianTransformer` (replaced by `DebianDiff`)
- ❌ **Removed**: `DebianLoader` (replaced by `DebianDB`) 
- ✅ **Kept**: `DebianParser` (reused as-is)
- ✅ **Kept**: Fetching logic (GZipFetcher)
- ✅ **Enhanced**: Main pipeline with diff approach

The new approach maintains full compatibility with existing debian package/source file formats while dramatically improving efficiency and maintainability.