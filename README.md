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

## Multi-tenant Design
- Users are linked to one tenant via `UserTenant`.
- Superuser can select any tenant.
- Normal user can access only own tenant.

## API Endpoints
- `POST /api/sync/` start sync
- `GET /api/data/` get data
- `GET /api/profiling/` get profiling stats

### Auth
All API calls require Basic Auth.

Example:
```
curl -u admin:admin123 http://localhost:8000/api/data?tenant_id=BANK001&loan_type=RETAIL&dataset_type=CREDIT
```

### Sync
```
POST /api/sync/
Content-Type: application/json
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
```

### Profiling
```
GET /api/profiling?tenant_id=BANK001&loan_type=RETAIL&dataset_type=CREDIT
```

## External Bank Simulation
- `POST /external-bank/upload-csv/` upload CSV
- `GET /external-bank/current/` get current data

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

## Notes
- Max file size depends on your environment.
- Data is replaced, not appended.