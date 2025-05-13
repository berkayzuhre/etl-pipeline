# ETL Pipeline with Prefect, AWS S3, and PostgreSQL

A data pipeline that extracts LinkedIn profile data from AWS S3, transforms it using Pandas, and loads it into PostgreSQL using SQLAlchemy and Prefect for workflow orchestration.

## Features

- Extract data from AWS S3 bucket (CSV format)
- Transform data with custom business logic:
  - Determine seniority level from job titles
  - Flag technical profiles based on GitHub presence
  - Extract email domains
  - Track multiple email addresses
- Load data into PostgreSQL database
- Workflow orchestration with Prefect
- Docker support for PostgreSQL database

## Prerequisites

- Python 3.10 or higher
- Docker and Docker Compose
- AWS account with S3 access
- Poetry for dependency management

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd etl-pipeline
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Create a `.env` file with the following variables:
```bash
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=your_aws_region
S3_BUCKET=your_bucket_name
S3_FILE_KEY=linkedin_data.csv
DB_CONNECTION=postgresql://postgres:postgres@localhost:5432/etl_db
```

## Usage

1. Start the PostgreSQL database:
```bash
docker-compose up -d
```

2. Run the ETL pipeline:
```bash
poetry run python main.py
```

## Project Structure

- `etl_pipeline`
  - `etl_pipeline/extract.py`: S3 data extraction logic
  - `etl_pipeline/transform.py`: Data transformation and enrichment
  - `etl_pipeline/load.py`: PostgreSQL data loading
  - `etl_pipeline/models.py`: SQLAlchemy database models
  - `etl_pipeline/business_logic.py`: Business rules for data transformation
  - `etl_pipeline/pipeline.py`: Prefect workflow definition

## Data Model

The pipeline processes LinkedIn profile data with the following fields:
- Basic profile information (name, title, company, etc.)
- Contact information (email, social media)
- Company details (LinkedIn URL, domain)
- Derived fields (seniority level, technical profile status)

## Development

- Format code with Black:
```bash
poetry run black .
```

- Sort imports with isort:
```bash
poetry run isort .
```

- Run type checking with mypy:
```bash
poetry run mypy .
```

## Dependencies

- Prefect: Workflow orchestration
- Pandas: Data manipulation
- SQLAlchemy: Database ORM
- Boto3: AWS S3 interaction
- Poetry: Dependency management
- PostgreSQL: Data storage

## License

MIT License - See LICENSE file for details
