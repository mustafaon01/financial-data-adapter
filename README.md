# Financial Data Adapter

This project syncs loan data from a mock bank and loads it to a data warehouse.

## Tech Stack
- Django (API + UI)
- PostgreSQL (app storage)
- ClickHouse (warehouse)
- Airflow (scheduled sync)
- GitHub Actions (quality checks)

## Setup
1. Copy `.env` and fill database settings.
2. Install packages:
   ```
   pip install -r requirements.txt
   ```
3. Run migrations:
   ```
   python manage.py migrate
   ```
4. Start server:
   ```
   python manage.py runserver
   ```
5. Or run with Docker:
   ```
   docker compose up --build
   ```
6. Docker image can also be shared and loaded with `docker load -i <image>.tar`.

## Multi-tenant Design
- Users are linked to one tenant via `UserTenant`.
- Superuser can select any tenant.
- Normal user can access only own tenant.

## API Endpoints
- `POST /api/sync/` start sync
- `GET /api/data/` get data
- `GET /api/profiling/` get profiling stats

### Auth
All API calls require Basic Auth. Use tenant-based users or superuser.

Example:
```
curl -u admin:admin123 http://localhost:8000/api/data?tenant_id=BANK001&loan_type=RETAIL&dataset_type=CREDIT
```

### Sync
```
POST /api/sync/
Content-Type: application/json
Authorization: Basic <base64>
Body:
{
  "tenant_id": "BANK001",
  "loan_type": "RETAIL",
  "dataset_type": "CREDIT"
}
```

### Data
```
GET /api/data?tenant_id=BANK001&loan_type=RETAIL&dataset_type=CREDIT
Authorization: Basic <base64>
```
Seed users (from `scripts/create_tenant_and_users.py`):
- `admin` / `admin123` (superuser)
- `bank001_user` / `test123` (BANK001)
- `bank002_user` / `test123` (BANK002)
- `bank003_user` / `test123` (BANK003)

The seed script is added to make demo and testing easier.

### Profiling
```
GET /api/profiling?tenant_id=BANK001&loan_type=RETAIL&dataset_type=CREDIT
Authorization: Basic <base64>
```

## External Bank Simulation
- `POST /external-bank/upload-csv/` upload CSV
- `GET /external-bank/current/` get current data

### Upload CSV
```
POST /external-bank/upload-csv/
Content-Type: multipart/form-data
Authorization: Basic <base64>
Fields:
- bank_code: BANK001
- loan_type: RETAIL
- dataset_type: CREDIT
- file: your.csv
```

## Tests
Run tests with:
```
USE_SQLITE_FOR_TESTS=1 pytest
```

## Quality
GitHub Actions runs:
- Ruff lint and format check
- Interrogate docstring coverage
- Pytest

## Obfuscation
We use PyArmor to obfuscate Python code before building the image.
This makes reverse engineering harder but does not give full protection.

## Notes
- Large CSV files are processed in chunks to reduce memory.
- New data replaces old data, it is not appended.

## Challenges and Changes
At first I tried `django-tenants`. It worked but it was harder to manage.
It also made the case more complex than required.
So I removed it and used a simpler tenant mapping with `UserTenant`.

## .env Note
The `.env` file is not in `.gitignore` to make review and setup easier.