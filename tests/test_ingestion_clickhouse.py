"""Tests for ingestion with ClickHouse mock."""

import uuid
from types import SimpleNamespace

import pytest

from adapter.ingestion import IngestionService
from api.models import Batch, Client, Loan
from external_bank.models import MockLoan


class FakeClickHouseClient:
    """Fake ClickHouse client for tests."""

    instances = []

    def __init__(self, tenant_schema=None):
        self.tenant_schema = tenant_schema
        self.queries = []
        self.inserts = []
        self.swaps = []
        FakeClickHouseClient.instances.append(self)

    def create_database(self, tenant_schema):
        self.queries.append(f"CREATE DATABASE {tenant_schema}")

    def execute_query(self, query, params=None):
        self.queries.append(query)
        return SimpleNamespace(result_rows=[], column_names=[])

    def insert_data(self, table, data, column_names=None):
        self.inserts.append(
            {"table": table, "data": data, "column_names": column_names}
        )

    def swap_tables(self, table_main, table_staging):
        self.swaps.append((table_main, table_staging))


@pytest.mark.django_db
def test_ingestion_loads_clickhouse_and_replaces_loans(monkeypatch):
    """Valid data should replace and load."""
    monkeypatch.setattr("adapter.ingestion.ClickHouseClient", FakeClickHouseClient)
    FakeClickHouseClient.instances = []

    tenant = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    Loan.objects.create(
        tenant=tenant,
        external_id="OLD_1",
        loan_type="RETAIL",
        amount="100.00",
        interest_rate="0.00",
        customer_name="Old",
        is_active=True,
    )

    MockLoan.objects.create(
        bank_code="BANK001",
        loan_type="RETAIL",
        external_id="LOAN_1",
        customer_id="C1",
        payload={
            "loan_account_number": "LOAN_1",
            "loan_status_code": "A",
            "loan_start_date": "2025-01-01",
            "original_loan_amount": "1000",
        },
        dataset_version=uuid.uuid4(),
    )
    MockLoan.objects.create(
        bank_code="BANK001",
        loan_type="RETAIL",
        external_id="LOAN_2",
        customer_id="C2",
        payload={
            "loan_account_number": "LOAN_2",
            "loan_status_code": "A",
            "loan_start_date": "2025-01-02",
            "original_loan_amount": "2000",
        },
        dataset_version=uuid.uuid4(),
    )

    service = IngestionService()
    ok = service.run_ingestion(
        tenant_id="BANK001", loan_type="RETAIL", dataset_type="CREDIT"
    )
    assert ok is True

    batch = Batch.objects.order_by("-created_at").first()
    assert batch.record_count == 2
    assert Loan.objects.filter(tenant=tenant, loan_type="RETAIL").count() == 2

    assert len(FakeClickHouseClient.instances) >= 1
    ch = FakeClickHouseClient.instances[-1]
    assert ch.inserts
    assert any("fact_loans_current_retail" in q for q in ch.queries)


@pytest.mark.django_db
def test_invalid_data_does_not_replace_existing(monkeypatch):
    """Invalid data should keep old data."""
    monkeypatch.setattr("adapter.ingestion.ClickHouseClient", FakeClickHouseClient)
    FakeClickHouseClient.instances = []

    tenant = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    Loan.objects.create(
        tenant=tenant,
        external_id="KEEP_1",
        loan_type="RETAIL",
        amount="300.00",
        interest_rate="0.00",
        customer_name="Keep",
        is_active=True,
    )

    MockLoan.objects.create(
        bank_code="BANK001",
        loan_type="RETAIL",
        external_id="BAD_1",
        customer_id="C1",
        payload={
            "loan_account_number": "BAD_1",
            "loan_status_code": "A",
            "loan_start_date": "2025-01-01",
            # missing original_loan_amount
        },
        dataset_version=uuid.uuid4(),
    )

    service = IngestionService()
    ok = service.run_ingestion(
        tenant_id="BANK001", loan_type="RETAIL", dataset_type="CREDIT"
    )
    assert ok is False

    assert Loan.objects.filter(tenant=tenant, loan_type="RETAIL").count() == 1
    assert len(FakeClickHouseClient.instances) == 0
