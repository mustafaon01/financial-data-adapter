"""Models for API app."""

import uuid

from django.conf import settings
from django.db import models


class Client(models.Model):
    """
    Tenant (Bank) model.
    Example: BANK001, BANK002, BANK003
    """

    name = models.CharField(max_length=100)
    tenant_code = models.CharField(max_length=10, unique=True, db_index=True)  # BANK001

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.tenant_code})"


class UserTenant(models.Model):
    """
    User to tenant mapping.
    """

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="tenant_link",
    )
    tenant = models.ForeignKey(
        Client,
        on_delete=models.CASCADE,
        related_name="users",
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user"], name="uniq_user_tenant_link"),
        ]

    def __str__(self):
        return f"{self.user.username} -> {self.tenant.tenant_code}"


class BatchStatus(models.TextChoices):
    STARTED = "STARTED", "Started"
    PROCESSING = "PROCESSING", "Processing"
    SUCCESS = "SUCCESS", "Success"
    FAILED = "FAILED", "Failed"
    FAILED_VALIDATION = "FAILED_VALIDATION", "Failed Validation"


class Batch(models.Model):
    """
    Audit log for ingestion batches.
    """

    BatchStatus = BatchStatus

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="batches")
    status = models.CharField(
        max_length=20,
        choices=BatchStatus.choices,
        default=BatchStatus.STARTED,
        db_index=True,
    )
    loan_type = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    record_count = models.IntegerField(default=0)
    total_rows = models.IntegerField(default=0)
    valid_rows = models.IntegerField(default=0)
    invalid_rows = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    error_message = models.TextField(null=True, blank=True)

    def __str__(self):
        return f"Batch {self.id} - {self.tenant.tenant_code} - {self.status}"


class BatchError(models.Model):
    """
    Store row validation errors.
    """

    batch = models.ForeignKey(Batch, related_name="errors", on_delete=models.CASCADE)
    row_number = models.IntegerField()
    error_code = models.CharField(max_length=50)  # e.g. NEGATIVE_AMOUNT
    field_name = models.CharField(max_length=50, null=True, blank=True)
    message = models.TextField()
    raw_excerpt = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["row_number"]
        indexes = [
            models.Index(fields=["batch", "row_number"]),
        ]

    def __str__(self):
        return f"{self.batch_id} row={self.row_number} code={self.error_code}"


class Loan(models.Model):
    """Loan snapshot."""

    LOAN_TYPES = [
        ("RETAIL", "Retail Loan"),
        ("COMMERCIAL", "Commercial Loan"),
    ]

    tenant = models.ForeignKey(
        "api.Client", on_delete=models.CASCADE, related_name="loans", db_index=True
    )
    external_id = models.CharField(max_length=100)
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES, db_index=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    interest_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    customer_name = models.CharField(max_length=255, blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "external_id", "loan_type"], name="uniq_tenant_loan"
            ),
        ]
        indexes = [
            models.Index(fields=["tenant", "loan_type"]),
            models.Index(fields=["tenant", "external_id"]),
        ]

    def __str__(self):
        return f"{self.tenant.tenant_code} {self.loan_type} {self.external_id}"


class LoanPaymentPlan(models.Model):
    """
    Installment rows for payment plan.
    """

    LOAN_TYPES = [
        ("RETAIL", "Retail Loan"),
        ("COMMERCIAL", "Commercial Loan"),
    ]

    tenant = models.ForeignKey(
        "api.Client",
        on_delete=models.CASCADE,
        related_name="payment_plans",
        db_index=True,
    )
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES, db_index=True)
    loan_external_id = models.CharField(max_length=100, db_index=True)
    installment_number = models.IntegerField(db_index=True)
    scheduled_payment_date = models.DateField(null=True, blank=True)
    installment_amount = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "tenant",
                    "loan_type",
                    "loan_external_id",
                    "installment_number",
                ],
                name="uniq_tenant_payment_plan_row",
            )
        ]
        indexes = [
            models.Index(fields=["tenant", "loan_type"]),
            models.Index(fields=["tenant", "loan_external_id"]),
        ]

    def __str__(self):
        return f"{self.tenant.tenant_code} {self.loan_type} {self.loan_external_id} #{self.installment_number}"
