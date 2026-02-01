from django.db import models
from django_tenants.models import TenantMixin, DomainMixin


class Client(TenantMixin):
    """
    Represent each tenant/bank
    """

    name = models.CharField(max_length=100)
    tenant_code = models.CharField(max_length=10, unique=True)
    created_on = models.DateField(auto_now_add=True)
    # Create automaticly schema
    auto_create_schema = True

    def __str__(self):
        return f"{self.name} ({self.tenant_code})"


class Domain(DomainMixin):
    """
    It's mandatory to create according to django-tenant domain control even if we won't use
    """

    pass


class Loan(models.Model):
    """
    Load Data
    """

    LOAN_TYPES = [
        ("RETAIL", "Retail Loan"),
        ("COMMERCIAL", "Commercial Loan"),
    ]

    external_id = models.CharField(max_length=100)
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    interest_rate = models.DecimalField(max_digits=5, decimal_places=2)
    customer_name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("external_id", "loan_type")

    def __str__(self):
        return f"{self.external_id} - {self.amount}"
