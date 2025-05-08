# Prometheus: Add README for chai

## Project Overview

CHAI is an innovative open-source data pipeline designed to aggregate and normalize package manager data across different ecosystems. The project aims to create a unified, comprehensive database of software package information that can be used for various analytical and research purposes.

### Core Purpose

The primary goal of CHAI is to develop a flexible data infrastructure that can:
- Extract package metadata from multiple package managers
- Provide a normalized, consistent data schema for package information
- Enable in-depth analysis of open-source software dependencies

### Key Features

- **Multi-Package Manager Support**: Currently supports Crates and Homebrew, with plans to expand to NPM, PyPI, and RubyGems
- **Comprehensive Dependency Mapping**: Creates a detailed dependency graph of open-source packages
- **Flexible Data Schema**: Designed to accommodate metadata from various package ecosystems
- **Extensible Architecture**: Modular pipeline allowing easy addition of new package managers

### Potential Use Cases

The project enables multiple advanced software analysis scenarios, including:
- License compatibility checking
- Developer publication tracking
- Package popularity analysis
- Dependency vulnerability assessment

By providing a standardized approach to package data collection and analysis, CHAI serves as a powerful tool for researchers, developers, and organizations seeking deep insights into the open-source software ecosystem.

## Getting Started, Installation, and Setup

### Prerequisites

- [Docker](https://docker.com)
- Git
- Terminal/Command Line

### Quick Start

1. Clone the CHAI repository:
   ```bash
   git clone https://github.com/your-organization/chai.git
   cd chai
   ```

2. Build the Docker images:
   ```bash
   docker compose build
   ```

3. Start the CHAI pipeline:
   ```bash
   docker compose up
   ```

### Package Manager Selection

By default, CHAI runs for all supported package managers. Currently, this includes:
- Crates
- Homebrew

To run a specific package manager, use:
```bash
docker compose up <package_manager>
```

### Configuration Options

You can customize the pipeline behavior using environment variables:

| Variable    | Description                                           | Default |
|-------------|-------------------------------------------------------|---------|
| `FREQUENCY` | How often (in hours) the pipeline should run          | -       |
| `TEST`      | Run loader in test mode, skipping certain data insertions | `false` |
| `FETCH`     | Fetch new data from source                            | `true`  |
| `NO_CACHE`  | Delete temporary files after processing               | `false` |

Example:
```bash
FREQUENCY=24 FETCH=true docker compose up
```

### Development Notes

- Initial data fetch can take considerable time (e.g., Crates may take over an hour)
- Database storage requirements vary by package manager
- Planned future support for NPM, PyPI, and RubyGems

### Troubleshooting

- To reset data: `rm -rf ./data`
- Database connection: `postgresql://postgres:s3cr3t@localhost:5435/chai`