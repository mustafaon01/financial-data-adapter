from django.db import models
import uuid


class DatasetState(models.Model):
    """
    Tracks the version of the dataset for a given bank and loan type.
    Updated on every CSV upload.
    """
    DATA_TYPES = [
        ('RETAIL', 'Retail Data'),
        ('COMMERCIAL', 'Commercial Data'),
    ]

    bank_code = models.CharField(max_length=20, db_index=True)
    loan_type = models.CharField(max_length=20, choices=DATA_TYPES)
    dataset_version = models.UUIDField(default=uuid.uuid4)
    checksum = models.CharField(max_length=64, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['bank_code', 'loan_type'], name='unique_dataset_state')
        ]
        db_table = 'external_bank_dataset_state'


class MockLoan(models.Model):
    """
    Represents the Current State of a loan in the External Bank system.
    """
    DATA_TYPES = [
        ('RETAIL', 'Retail Data'),
        ('COMMERCIAL', 'Commercial Data'),
    ]

    bank_code = models.CharField(max_length=20, db_index=True)
    loan_type = models.CharField(max_length=20, choices=DATA_TYPES)

    external_id = models.CharField(max_length=100, db_index=True)
    customer_id = models.CharField(max_length=100, null=True, blank=True)

    payload = models.JSONField()
    dataset_version = models.UUIDField(db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['bank_code', 'loan_type', 'external_id'], name='unique_mock_loan')
        ]
        db_table = 'external_bank_mock_loan'
        indexes = [
            models.Index(fields=['bank_code', 'loan_type']),
        ]
