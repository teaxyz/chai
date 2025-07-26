# CHAI API

CHAI API is a REST API service for accessing the CHAI database, which contains package
manager data.

## Features

- List all tables in the database
- Fetch paginated data from any table
- Heartbeat endpoint for health checks
- Search deduplicated packages by name

## Requirements

- Rust 1.67 or later
- PostgreSQL database

## API Endpoints

### Health Check

```
GET /heartbeat
```

Returns the health status of the API and database connection.

**Response (Success)**

```txt
OK - Database connection is healthy
```

**Response (Failure - Database query failed):**

```txt
Database query failed
```

**Response (Failure - Database connection failed):**

```txt
Failed to get database connection
```

### List Tables

```
GET /tables
```

Returns a paginated list of all available tables in the database.

**Query Parameters**

- `page` (optional): Page number (default: 1)
- `limit` (optional): Number of items per page (default: 200)

**Response**

```json
{
  "data": [
    "legacy_dependencies",
    "versions",
    "canons_old",
    "tea_rank_runs",
    "canons",
    "licenses",
    "canon_packages",
    "users",
    "load_history",
    "tea_ranks",
    "alembic_version",
    "sources",
    "package_managers",
    "url_types",
    "urls",
    "packages",
    "package_urls",
    "user_packages",
    "dependencies",
    "depends_on_types",
    "user_versions",
    "canon_packages_old",
    "tea_rank_old"
  ],
  "limit": 200,
  "page": 1,
  "total_count": 23,
  "total_pages": 1
}
```

### Get Table Data

```
GET /{table}
```

Returns paginated data from the specified table.

**Path Parameters**

- `table`: Name of the table to query (see available tables in List Tables response)

**Query Parameters**

- `page` (optional): Page number (default: 1)
- `limit` (optional): Number of items per page (default: 200)

**Response**

```json
{
    "table": "packages",
    "total_count": 166459,
    "page": 1,
    "limit": 2,
    "total_pages": 83230,
    "columns": [
        ...
    ],
    "data": [
        {
            "created_at": "2024-12-27 08:04:03.991832",
            "derived_id": "...",
            "id": "...",
            "import_id": "...",
            "name": "...",
            "package_manager_id": "...",
            "readme": "...",
            "updated_at": "2024-12-27 08:04:03.991832"
        },
        ...
    ]
}
```

### Get Table Row By ID

```
GET /{table}/{id}
```

Returns a specific row from the table by its UUID.

**Path Parameters**

- `table`: Name of the table to query
- `id`: UUID of the row to fetch

**Response**

```json
{
  "created_at": "2024-12-27 08:04:03.991832",
  "derived_id": "...",
  "id": "...",
  "import_id": "...",
  "name": "...",
  "package_manager_id": "...",
  "readme": "...",
  "updated_at": "2024-12-27 08:04:03.991832"
}
```

### Get Project

```
GET /project/{id}
```

Returns detailed information about a specific canon by its canonical ID.

**Path Parameters**

- `id`: UUID of the project (canon) to fetch

**Response**

```json
{
  "projectId": "550e8400-e29b-41d4-a716-446655440000",
  "homepage": "https://example.com",
  "name": "example-project",
  "source": "https://github.com/example/project",
  "teaRank": "150",
  "teaRankCalculatedAt": "2024-12-27T08:04:03.991832",
  "packageManagers": ["homebrew", "crates"]
}
```

**Response (Not Found)**

```json
{
  "error": "No row found with id '550e8400-e29b-41d4-a716-446655440000' in table canons"
}
```

### Get Projects Batch

```
POST /project/batch
```

Returns detailed information about multiple projects by their canonical IDs.

**Request Body**

```json
{
  "projectIds": ["uuid1", "uuid2", "..."]
}
```

**Parameters**

- `projectIds`: Array of project UUIDs to include in the leaderboard (required, max 100)

**Example**

```
POST /project/batch
```

**Example Request**

```bash
curl -X POST http://localhost:8080/project/batch \
  -H "Content-Type: application/json" \
  -d '{
    "projectIds": [
      "550e8400-e29b-41d4-a716-446655440000",
      "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    ]
  }'
```

**Response**

```json
[
  {
    "projectId": "550e8400-e29b-41d4-a716-446655440000",
    "homepage": "https://example.com",
    "name": "example-project",
    "source": "https://github.com/example/project",
    "teaRank": "150",
    "teaRankCalculatedAt": "2024-12-27T08:04:03.991832",
    "packageManagers": ["homebrew", "crates"]
  },
  {
    "projectId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "homepage": "https://another-example.com",
    "name": "another-project",
    "source": "https://github.com/another/project",
    "teaRank": "75",
    "teaRankCalculatedAt": "2024-12-26T10:15:22.123456",
    "packageManagers": ["debian", "pkgx"]
  }
]
```

**Response (Invalid UUIDs)**

```json
{
  "error": "Invalid UUID format in project IDs"
}
```

### Search Projects

```
GET /project/search/{name}
```

Searches for projects by name using case-insensitive partial matching. Results are
ordered by name length and limited to 10 items.

**Path Parameters**

- `name`: Project name to search for (partial matches supported)

**Example**

```
GET /project/search/python
```

**Response**

```json
[
  {
    "projectId": "550e8400-e29b-41d4-a716-446655440000",
    "homepage": "https://reactjs.org",
    "name": "react",
    "source": "https://github.com/facebook/react",
    "packageManagers": ["homebrew", "npm"]
  },
  {
    "projectId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "homepage": "https://reactrouter.com",
    "name": "react-router",
    "source": "https://github.com/remix-run/react-router",
    "packageManagers": ["npm"]
  }
]
```

**Response (Empty Search)**

```json
{
  "error": "Search name cannot be empty"
}
```

### Leaderboard

```
POST /leaderboard
```

Returns detailed information about specified projects, ordered by tea rank in descending
order. This endpoint allows filtering by project IDs and limiting the number of results.

**Request Body**

```json
{
  "projectIds": ["uuid1", "uuid2", "..."],
  "limit": 10
}
```

**Parameters**

- `projectIds`: Array of project UUIDs to include in the leaderboard (required, max 100)
- `limit`: Maximum number of results to return (required, 1-100)

**Example Request**

```bash
curl -X POST http://localhost:8080/leaderboard \
  -H "Content-Type: application/json" \
  -d '{
    "projectIds": [
      "1e233f1b-2b49-4ada-9953-1763785fba2c",
      "2c24aa45-4fe2-4f2b-ae58-09d4b9a4ad28"
    ],
    "limit": 2
  }'
```

**Response**

```json
[
  {
    "projectId": "1e233f1b-2b49-4ada-9953-1763785fba2c",
    "homepage": "https://example.com",
    "name": "example-project",
    "source": "https://github.com/example/project",
    "teaRank": "150",
    "teaRankCalculatedAt": "2024-12-27T08:04:03.991832",
    "packageManagers": ["homebrew", "crates"]
  },
  {
    "projectId": "2c24aa45-4fe2-4f2b-ae58-09d4b9a4ad28",
    "homepage": "https://another-example.com",
    "name": "another-project",
    "source": "https://github.com/another/project",
    "teaRank": "75",
    "teaRankCalculatedAt": "2024-12-26T10:15:22.123456",
    "packageManagers": ["debian", "pkgx"]
  }
]
```

**Response (Validation Errors)**

```json
{
  "error": "At least one project ID is required"
}
```

```json
{
  "error": "Too many project IDs (maximum 100 allowed)"
}
```

```json
{
  "error": "Invalid limit 150: must be between 1 and 100"
}
```

## Available Tables

The database contains the following tables:

| Table Name       | Description                                      |
| ---------------- | ------------------------------------------------ |
| alembic_version  | Store the current version of alembic             |
| dependencies     | Package dependencies                             |
| depends_on_types | Types of package dependencies                    |
| licenses         | Package licenses                                 |
| load_history     | Load history                                     |
| package_managers | Package manager information                      |
| package_urls     | Relationship of packages to URLs                 |
| packages         | Package metadata                                 |
| sources          | Package manager sources (homebrew, crates, etc.) |
| url_types        | Types of URLs (homepage, repository, etc.)       |
| urls             | Actual URLs                                      |
| user_packages    | User-package relationships                       |
| user_versions    | User-version relationships                       |
| users            | User (package owner) information                 |
| versions         | Package versions                                 |

By default, the API will be available at `http://localhost:8080`.

## Deployment

The CHAI API is deployed using AWS services with the following stack:

- **Amazon ECR (Elastic Container Registry)** - Container image storage
- **Amazon ECS (Elastic Container Service)** - Container orchestration
- **ECS Service** - Manages running tasks and load balancing
- **ECS Task Definition** - Defines container configuration

### Prerequisites

- AWS CLI configured with appropriate permissions
- Docker installed locally
- Access to the AWS account and ECR repository

### Building and Pushing Docker Image

1. **Get ECR login credentials:**

   ```bash
   aws ecr get-login-password --region <your-region> | docker login --username AWS --password-stdin <account-id>.dkr.ecr.<your-region>.amazonaws.com
   ```

2. **Build the Docker image:**

   ```bash
   docker build -t chai-api .
   ```

3. **Tag the image for ECR:**

   ```bash
   docker tag chai-api:latest <account-id>.dkr.ecr.<your-region>.amazonaws.com/chai-api:latest
   ```

4. **Push the image to ECR:**

   ```bash
   docker push <account-id>.dkr.ecr.<your-region>.amazonaws.com/chai-api:latest
   ```

   > **Note:** Replace `<account-id>` and `<your-region>` with your AWS account ID and region. You can find the exact commands in your ECR repository console under "View push commands".

### Updating Existing ECS Service

If updating the ECS service, you first need to Build and Push the docker image. Then:

```bash
aws ecs update-service --cluster chai-<environment> --service <environment>-chai-api --force-new-deployment
```

### Environment Variables

Ensure the following environment variables are configured in your task definition:

- `DATABASE_URL`: PostgreSQL connection string
- `HOST`: Host to bind to (default: "0.0.0.0")
- `PORT`: Port to listen on (default: "8080")

### Useful AWS Documentation

- [Amazon ECR User Guide](https://docs.aws.amazon.com/ecr/)
- [Amazon ECS Developer Guide](https://docs.aws.amazon.com/ecs/)
- [ECS Task Definitions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definitions.html)
- [ECS Services](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html)
- [AWS CLI ECS Commands](https://docs.aws.amazon.com/cli/latest/reference/ecs/)

## Tasks

### Format

```bash
cargo fmt --all --
```

### Build

```bash
cargo build --release
```

### Validate

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Run

Env: DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5435/chai

```bash
target/release/chai-api
```
