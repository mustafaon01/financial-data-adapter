#!/bin/sh
set -e

echo "###Applying database migrations###"
python manage.py migrate --noinput

echo "###Creating new tenants and users...###"
python "manage.py" shell -c "from scripts.create_tenant_and_users import run; run()"

echo "###Starting application###"
exec gunicorn financial_data_adapter.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers 2 \
  --threads 4
