"""Pytest setup for Django settings."""

import os
import sys

import django


def pytest_configure():
    """Configure Django settings for tests."""
    root = os.path.dirname(os.path.abspath(__file__))
    if root not in sys.path:
        sys.path.insert(0, root)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "financial_data_adapter.settings")
    django.setup()


def django_db_setup(django_db_blocker):
    """Ensure migrations are applied in tests."""
    from django.core.management import call_command

    with django_db_blocker.unblock():
        call_command("migrate", verbosity=0)
