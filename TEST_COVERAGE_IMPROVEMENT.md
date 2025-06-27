# Test Coverage Improvement for Package Indexers

Systematic improvement of test coverage to achieve 100% coverage for core modules and package manager indexers (homebrew, pkgx, crates).

## Completed Tasks
- [x] Initial coverage analysis from htmlcov report
- [x] Created task tracking file
- [x] Created comprehensive tests for core/db.py (24% → 92% coverage) 🚀
- [x] Created comprehensive tests for core/fetcher.py (31% → 83% coverage) 🚀  
- [x] Created comprehensive tests for core/transformer.py (42% → 100% coverage) 🚀
- [x] Created comprehensive tests for core/utils.py (33% → 100% coverage) 🚀

## In Progress Tasks
- [ ] Fix failing test cases and improve test mocking
- [ ] Review and improve core/config.py coverage (still 64%)

## Future Tasks
- [ ] Improve core/transformer.py coverage (42% → 100%)
- [ ] Improve core/utils.py coverage (33% → 100%)
- [ ] Improve core/config.py coverage (64% → 100%)
- [ ] Improve package_managers/crates/db.py coverage (19% → 100%)
- [ ] Improve package_managers/crates/diff.py coverage (56% → 100%)  
- [ ] Improve package_managers/crates/main.py coverage (15% → 100%)
- [ ] Improve package_managers/crates/transformer.py coverage (13% → 100%)
- [ ] Improve package_managers/homebrew/diff.py coverage (62% → 100%)
- [ ] Improve package_managers/pkgx/parser.py coverage (37% → 100%)
- [ ] Improve package_managers/pkgx/url.py coverage (46% → 100%)
- [ ] Improve package_managers/pkgx/db.py coverage (54% → 100%)
- [ ] Run final coverage validation

## Coverage Analysis

### Current Coverage Status (Updated)
- **Core modules:**
  - core/config.py: 64% (37/103 missing lines) - Unchanged, needs work
  - core/db.py: 92% (11/146 missing lines) ✅ **+68% improvement!**
  - core/fetcher.py: 83% (18/104 missing lines) ✅ **+52% improvement!** 
  - core/logger.py: 73% (11/41 missing lines) - Slightly decreased 
  - core/models/__init__.py: 95% (9/184 missing lines) ✅
  - core/structs.py: 100% ✅
  - core/transformer.py: 100% (0/31 missing lines) ✅ **+58% improvement!**
  - core/utils.py: 100% (0/27 missing lines) ✅ **+67% improvement!**

- **Homebrew package manager:**
  - diff.py: 62% (40/104 missing lines)
  - structs.py: 100% ✅

- **pkgx package manager:**
  - db.py: 54% (6/13 missing lines)
  - diff.py: 85% (15/100 missing lines) 
  - parser.py: 37% (123/195 missing lines) 🔴
  - url.py: 46% (33/61 missing lines) 🔴

- **Crates package manager:**
  - db.py: 19% (50/62 missing lines) 🔴
  - diff.py: 56% (48/110 missing lines) 🔴
  - main.py: 15% (66/78 missing lines) 🔴
  - structs.py: 100% ✅
  - transformer.py: 13% (81/93 missing lines) 🔴

### Priority Areas (Low Coverage <50%)
1. **Critical (Core)**: core/db.py (24%), core/fetcher.py (31%), core/utils.py (33%), core/transformer.py (42%)
2. **Critical (Crates)**: All crates modules except structs.py
3. **High (pkgx)**: parser.py (37%), url.py (46%)
4. **Medium**: homebrew/diff.py (62%), core/config.py (64%)

### **Total Coverage**: 83% for core modules (was ~40% average) → Target: 100%

**🎉 MAJOR PROGRESS: Core modules now have 83% average coverage vs. previous ~40%!**

## Implementation Plan

### Phase 1: Core Infrastructure (Critical)
Focus on core modules that are used across all package managers:

1. **core/db.py** - Database operations, connections, queries
2. **core/fetcher.py** - HTTP requests, caching, retry logic  
3. **core/utils.py** - Utility functions used throughout
4. **core/transformer.py** - Data transformation pipeline

### Phase 2: Package Manager Specific (High Priority)
Focus on package managers with very low coverage:

1. **Crates package manager** - Complete overhaul needed (13-19% coverage)
2. **pkgx parser and URL modules** - Complex parsing logic needs testing
3. **Homebrew diff module** - Dependency diffing algorithm

### Phase 3: Configuration and Edge Cases
1. **core/config.py** - Configuration loading and validation
2. **Remaining edge cases and error paths**

### Testing Strategy
- **Mock Strategy**: Use existing fixtures from conftest.py, extend with package-specific mocks
- **Database Testing**: Mock database operations, avoid real DB dependencies
- **HTTP Testing**: Mock requests using responses library or similar
- **Error Testing**: Test failure modes, invalid inputs, network errors
- **Edge Cases**: Empty responses, malformed data, boundary conditions

### Relevant Files
- tests/conftest.py - Main test fixtures and mocks ✅
- tests/package_managers/homebrew/conftest.py - Homebrew-specific mocks ✅
- tests/package_managers/crates/conftest.py - Crates-specific mocks ✅
- tests/core/ - **TO CREATE** - Core module tests
- tests/package_managers/pkgx/test_parser.py - **TO CREATE** - pkgx parser tests
- tests/package_managers/pkgx/test_url.py - **TO CREATE** - pkgx URL tests
- tests/package_managers/crates/test_db.py - **TO CREATE** - Crates DB tests
- tests/package_managers/crates/test_main.py - **TO CREATE** - Crates main tests
- tests/package_managers/crates/test_transformer.py - **TO CREATE** - Crates transformer tests

## Progress Tracking
- [x] **Phase 1 Core Infrastructure: 83% Complete** 🚀
  - [x] core/db.py: 92% ✅
  - [x] core/fetcher.py: 83% ✅
  - [x] core/transformer.py: 100% ✅
  - [x] core/utils.py: 100% ✅
  - [ ] core/config.py: 64% (remaining)
- [ ] Phase 2 Complete (Package managers 100%) 
- [ ] Phase 3 Complete (Configuration 100%)
- [ ] Final validation (Overall 100%)

### Next Immediate Steps
1. **Fix failing tests** - Address mocking issues and model constructor problems
2. **Core config.py** - Improve remaining 36% coverage 
3. **Package managers** - Move to crates, homebrew, pkgx modules
4. **Final validation** - Achieve 100% overall coverage