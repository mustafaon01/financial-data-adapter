"""Tests for login flow."""

import pytest
from django.contrib.auth import get_user_model

from api.models import Client, UserTenant


@pytest.mark.django_db
def test_normal_user_login_sets_tenant(client):
    """Normal user login sets tenant."""
    user_model = get_user_model()
    tenant = Client.objects.create(name="Bank 1", tenant_code="BANK001")
    user = user_model.objects.create_user(username="u1", password="pass123")
    UserTenant.objects.create(user=user, tenant=tenant)

    res = client.post("/login/", data={"username": "u1", "password": "pass123"})
    assert res.status_code == 302
    assert client.session.get("active_tenant") == "BANK001"


@pytest.mark.django_db
def test_superuser_needs_tenant(client):
    """Superuser must select tenant."""
    user_model = get_user_model()
    user_model.objects.create_superuser(
        username="admin", password="pass123", email="a@a.com"
    )

    res = client.post("/login/", data={"username": "admin", "password": "pass123"})
    assert res.status_code == 200
    assert b"Tenant selection is required for superuser." in res.content
