"""Tests for data endpoints."""

import pytest
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient

from api.models import Client, UserTenant


class FakeClickHouseClient:
    """Fake ClickHouse client for data tests."""

    def __init__(self, tenant_schema=None):
        self.tenant_schema = tenant_schema

    def execute_query(self, query, params=None):
        if "fact_loans_current_retail" in query:
            return type("Res", (), {"result_rows": [["LOAN_R1"]]})()
        if "fact_loans_current_commercial" in query:
            return type("Res", (), {"result_rows": [["LOAN_C1"]]})()
        return type("Res", (), {"result_rows": []})()


@pytest.mark.django_db
def test_retail_and_commercial_are_separate(monkeypatch):
    """Retail and commercial data are separate."""
    monkeypatch.setattr("api.views.ClickHouseClient", FakeClickHouseClient)

    user_model = get_user_model()
    tenant = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    user = user_model.objects.create_user(username="u1", password="pass123")
    UserTenant.objects.create(user=user, tenant=tenant)

    api = APIClient()
    api.force_authenticate(user=user)

    res_retail = api.get(
        "/api/data/",
        data={"tenant_id": "BANK001", "loan_type": "RETAIL", "dataset_type": "CREDIT"},
    )
    assert res_retail.status_code == 200
    assert res_retail.data["rows"] == [["LOAN_R1"]]

    res_comm = api.get(
        "/api/data/",
        data={"tenant_id": "BANK001", "loan_type": "COMMERCIAL", "dataset_type": "CREDIT"},
    )
    assert res_comm.status_code == 200
    assert res_comm.data["rows"] == [["LOAN_C1"]]
