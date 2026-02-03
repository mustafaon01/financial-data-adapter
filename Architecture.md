
```bash
tree -a -I ".git" 
```

    .
    ├── .env
    ├── .github
    │ └── workflows
    │     └── quality.yml
    ├── .gitignore
    ├── .idea
    ├── adapter
    │ ├── __init__.py
    │ ├── clickhouse_client.py
    │ ├── ingestion.py
    │ ├── normalizers.py
    │ ├── schemas.py
    │ └── validators.py
    ├── airflow
    │ ├── dags
    │ │ ├── __pycache__
    │ │ │ └── ingestion_dag.cpython-312.pyc
    │ │ └── ingestion_dag.py
    │ ├── logs
    │ └── plugins
    ├── api
    │├── __init__.py
    │├── admin.py
    │├── apps.py
    │├── migrations
    ││ ├── __init__.py
    ││ └── 0001_initial.py
    │├── models.py
    │├── serializers.py
    │├── tests.py
    │├── urls.py
    │└── views.py
    ├── Architecture.md
    ├── conftest.py
    ├── docker-compose.yml
    ├── Dockerfile
    ├── external_bank
    │ ├── __init__.py
    │ ├── admin.py
    │ ├── apps.py
    │ ├── csv_read_helpers.py
    │ ├── migrations
    │ │ ├── __init__.py
    │ │ └── 0001_initial.py
    │ ├── models.py
    │ ├── tests.py
    │ ├── urls.py
    │ └── views.py
    ├── financial_data_adapter
    │├── __init__.py
    │├── asgi.py
    │├── settings.py
    │├── urls.py
    │├── views.py
    │└── wsgi.py
    ├── manage.py
    ├── pyproject.toml
    ├── pytest.ini
    ├── README.md
    ├── requirements.txt
    ├── scripts
    │ ├── create_tenant_and_users.py
    │ └── entrypoint.sh
    ├── templates
    │ ├── base.html
    │ ├── components
    │ │ ├── footer.html
    │ │ └── navbar.html
    │ ├── dashboard.html
    │ ├── data_explorer.html
    │ ├── external_bank.html
    │ └── login.html
    └── tests
        ├── test_external_bank.py
        ├── test_ingestion_clickhouse.py
        ├── test_login.py
        └── test_validation.py

    33 directories, 130 files
# Architecture

## Overview
The system pulls data from a mock bank, validates it, and stores it in ClickHouse.

## CI and Quality
- GitHub Actions runs on every push.
- Ruff is used for lint and format checks.
- Interrogate checks docstring coverage.
- Pytest runs for automated tests.

## Flow
1. User uploads CSV to External Bank service.
2. Sync endpoint pulls data and validates rows.
3. Valid data is normalized and loaded to Postgres and ClickHouse.
4. UI shows batches, data, and profiling.

## Modules
- `external_bank` (Django app): CSV upload and current data endpoints
- `api` (Django app): sync, data, profiling
- `adapter` (helper): validation, normalization, ingestion
- `airflow`: scheduled sync every 10 minutes
- `.github/workflows/quality.yml`: lint and test pipeline

We use Django apps for request handling, routing, and models.
Helper modules keep core logic simple and reusable.

## Data Rules
- Required fields must exist
- Types and ranges are checked
- Payment plan must match a valid loan

## Validation and Schema
- `schemas.py` defines field names and types for each dataset.
- Validation checks required fields, data types, and ranges.
- Normalization fixes dates, rates, and categories.

## Memory Efficiency
- Ingestion uses chunked iterators.
- Data is inserted in small batches.
- Validation is done in one pass before load.

## ClickHouse Tenant Design
- Each tenant has its own ClickHouse database: `dwh_{tenant}`.
- Tables are created per tenant and swapped with staging.
- Only the latest snapshot is kept.

## django-tenants Note
The project originally used `django-tenants`.
We removed it to keep the setup simple and avoid complex schema migrations.

## Storage
- Postgres keeps latest data for app view
- ClickHouse keeps current snapshot tables
- New data replaces old data
14 directories, 38 files