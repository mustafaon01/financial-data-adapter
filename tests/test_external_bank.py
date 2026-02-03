"""Tests for external bank endpoints."""

import io

import pytest
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient

from api.models import Client, UserTenant
from external_bank.models import MockLoan


@pytest.mark.django_db
def test_upload_requires_auth():
    """Upload requires authentication."""
    api = APIClient()
    file_obj = io.BytesIO(b"loan_account_number,customer_id\nLOAN_1,CUST_1\n")
    res = api.post(
        "/external-bank/upload-csv/",
        data={
            "bank_code": "BANK001",
            "loan_type": "RETAIL",
            "dataset_type": "CREDIT",
            "file": file_obj,
        },
        format="multipart",
    )
    assert res.status_code == 401


@pytest.mark.django_db
def test_upload_allowed_for_own_tenant():
    """User can upload for own tenant."""
    user_model = get_user_model()
    tenant = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    user = user_model.objects.create_user(username="u1", password="pass123")
    UserTenant.objects.create(user=user, tenant=tenant)

    api = APIClient()
    api.force_authenticate(user=user)
    file_obj = io.BytesIO(b"loan_account_number,customer_id\nLOAN_1,CUST_1\n")
    res = api.post(
        "/external-bank/upload-csv/",
        data={
            "bank_code": "BANK001",
            "loan_type": "RETAIL",
            "dataset_type": "CREDIT",
            "file": file_obj,
        },
        format="multipart",
    )
    assert res.status_code == 200
    assert res.data.get("status") == "success"


@pytest.mark.django_db
def test_current_data_is_tenant_scoped():
    """Current data is limited to tenant."""
    user_model = get_user_model()
    tenant1 = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    # tenant2 = Client.objects.create(name="Bank 2", tenant_code="BANK002")
    user = user_model.objects.create_user(username="u1", password="pass123")
    UserTenant.objects.create(user=user, tenant=tenant1)

    MockLoan.objects.create(
        bank_code="BANK001",
        loan_type="RETAIL",
        external_id="LOAN_1",
        customer_id="C1",
        payload={"loan_account_number": "LOAN_1"},
        dataset_version="11111111-1111-1111-1111-111111111111",
    )
    MockLoan.objects.create(
        bank_code="BANK002",
        loan_type="RETAIL",
        external_id="LOAN_2",
        customer_id="C2",
        payload={"loan_account_number": "LOAN_2"},
        dataset_version="22222222-2222-2222-2222-222222222222",
    )

    api = APIClient()
    api.force_authenticate(user=user)
    res = api.get(
        "/external-bank/current/",
        data={"bank_code": "BANK001", "loan_type": "RETAIL", "dataset_type": "CREDIT"},
    )
    assert res.status_code == 200
    assert res.data["bank_code"] == "BANK001"
    assert all(r["loan_account_number"] == "LOAN_1" for r in res.data["data"])
