FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /build

COPY requirements.txt /build/requirements.txt
RUN pip install -r /build/requirements.txt pyarmor

COPY . /build

RUN pyarmor gen -O /build/obf -r \
  manage.py \
  financial_data_adapter \
  api \
  adapter \
  external_bank


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY --from=builder /build/obf /app
COPY --from=builder /build/templates /app/templates
COPY --from=builder /build/scripts /app/scripts
COPY --from=builder /build/airflow/dags /app/airflow/dags

RUN chmod +x /app/scripts/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["sh", "/app/scripts/entrypoint.sh"]
