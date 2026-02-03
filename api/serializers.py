"""Serializers for API responses."""

from rest_framework import serializers

from .models import Batch, BatchError


class SyncRequestSerializer(serializers.Serializer):
    """Validate sync request input."""

    tenant_id = serializers.CharField()
    loan_type = serializers.ChoiceField(choices=["RETAIL", "COMMERCIAL"])
    dataset_type = serializers.ChoiceField(
        choices=["CREDIT", "PAYMENT_PLAN"],
        required=False,
        default="CREDIT",
    )

    def validate_tenant_id(self, v):
        return v.strip().upper()


class BatchSerializer(serializers.ModelSerializer):
    """Serialize batch data."""

    tenant_id = serializers.CharField(source="tenant.tenant_code", read_only=True)

    class Meta:
        model = Batch
        fields = [
            "id",
            "tenant_id",
            "loan_type",
            "status",
            "total_rows",
            "valid_rows",
            "invalid_rows",
            "record_count",
            "created_at",
            "started_at",
            "completed_at",
            "error_message",
        ]


class BatchErrorSerializer(serializers.ModelSerializer):
    """Serialize batch error data."""

    class Meta:
        model = BatchError
        fields = [
            "row_number",
            "error_code",
            "field_name",
            "message",
            "raw_excerpt",
            "created_at",
        ]
