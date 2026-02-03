"""API viewsets for sync, data, and profiling."""

from django.utils import timezone
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from adapter.clickhouse_client import ClickHouseClient
from adapter.ingestion import IngestionService
from adapter.schemas import (
    get_categorical_fields,
    get_field_names,
    get_numeric_fields,
)

from .models import Batch, Client, UserTenant
from .serializers import BatchErrorSerializer, BatchSerializer, SyncRequestSerializer


class TenantResolverMixin:
    """
    Simple tenant and table resolver.
    """

    def resolve_tenant(self, request, tenant_id: str | None) -> Client | None:
        """
        Superuser can pick any tenant.
        Normal user can use own tenant only.
        """
        tenant_id = (tenant_id or "").strip().upper()

        if request.user.is_superuser:
            if not tenant_id:
                return None
            return Client.objects.filter(tenant_code__iexact=tenant_id).first()

        try:
            user_tenant = UserTenant.objects.select_related("tenant").get(
                user=request.user
            )
        except UserTenant.DoesNotExist:
            return None

        assigned = user_tenant.tenant
        if tenant_id and assigned.tenant_code.upper() != tenant_id:
            return "FORBIDDEN"

        return assigned

    def resolve_ch_table(self, dataset_type: str, loan_type: str) -> str:
        """
        Build ClickHouse table name.
        """
        dataset_type = (dataset_type or "").upper()
        loan_type = (loan_type or "").upper()
        if dataset_type == "CREDIT":
            prefix = "fact_loans_current"
        else:
            prefix = "fact_payment_plan_current"
        suffix = "retail" if loan_type == "RETAIL" else "commercial"
        return f"{prefix}_{suffix}"


class BatchViewSet(TenantResolverMixin, viewsets.ReadOnlyModelViewSet):
    """
    List batches and errors.
    """

    permission_classes = [IsAuthenticated]
    serializer_class = BatchSerializer

    def get_queryset(self):
        if self.request.user.is_superuser:
            return Batch.objects.select_related("tenant").order_by("-created_at")

        try:
            tenant = (
                UserTenant.objects.select_related("tenant")
                .get(user=self.request.user)
                .tenant
            )
        except UserTenant.DoesNotExist:
            return Batch.objects.none()

        return (
            Batch.objects.select_related("tenant")
            .filter(tenant=tenant)
            .order_by("-created_at")
        )

    @action(detail=True, methods=["get"], url_path="errors")
    def errors(self, request, pk=None):
        """
        Get errors for a batch.
        """
        batch = self.get_object()  # queryset already tenant-filtered for non-superuser
        return Response(BatchErrorSerializer(batch.errors.all(), many=True).data)


class SyncViewSet(TenantResolverMixin, viewsets.ViewSet):
    """
    Sync request endpoint.
    """

    permission_classes = [IsAuthenticated]

    @staticmethod
    def start_sync_async(
        batch: Batch, tenant: Client, loan_type: str, dataset_type: str | None
    ) -> None:
        """
        Start ingestion in background.
        """
        import threading

        def run():
            service = IngestionService()
            try:
                service.run_ingestion(
                    tenant_id=tenant.tenant_code,
                    loan_type=loan_type,
                    dataset_type=dataset_type,
                    batch_id=batch.id,
                )
            except Exception as e:
                Batch.objects.filter(id=batch.id).update(
                    status=Batch.BatchStatus.FAILED,
                    error_message=str(e),
                    completed_at=timezone.now(),
                )

        threading.Thread(target=run, daemon=True).start()

    def create(self, request):
        """
        POST /api/sync/
        """
        s = SyncRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        tenant_id = s.validated_data["tenant_id"]
        loan_type = s.validated_data["loan_type"]
        dataset_type = s.validated_data.get("dataset_type")

        tenant = self.resolve_tenant(request, tenant_id)
        if tenant == "FORBIDDEN":
            return Response(
                {"error": "You are not allowed to access this tenant"}, status=403
            )
        if not tenant:
            return Response(
                {"error": f"Tenant '{tenant_id}' not found or user not assigned"},
                status=404,
            )

        # Prevent concurrent sync per tenant+loan_type
        if Batch.objects.filter(
            tenant=tenant,
            loan_type=loan_type,
            status__in=[Batch.BatchStatus.STARTED, Batch.BatchStatus.PROCESSING],
        ).exists():
            return Response(
                {"error": "A sync is already running for this tenant/loan_type"},
                status=409,
            )

        batch = Batch.objects.create(
            tenant=tenant,
            loan_type=loan_type,
            status=Batch.BatchStatus.STARTED,
            started_at=timezone.now(),
        )

        self.start_sync_async(
            batch=batch,
            tenant=tenant,
            loan_type=loan_type,
            dataset_type=dataset_type,
        )

        return Response(BatchSerializer(batch).data, status=202)


class DataViewSet(TenantResolverMixin, viewsets.ViewSet):
    """
    Get data from ClickHouse.
    """

    permission_classes = [IsAuthenticated]

    def list(self, request):
        """
        List data with filters.
        """
        tenant_id = (request.query_params.get("tenant_id") or "").upper()
        loan_type = (request.query_params.get("loan_type") or "").upper()
        dataset_type = (request.query_params.get("dataset_type") or "").upper()

        if not tenant_id or loan_type not in ["RETAIL", "COMMERCIAL"]:
            return Response(
                {"error": "tenant_id and valid loan_type required"}, status=400
            )
        if dataset_type not in ["CREDIT", "PAYMENT_PLAN"]:
            return Response({"error": "valid dataset_type required"}, status=400)

        tenant = self.resolve_tenant(request, tenant_id)
        if tenant == "FORBIDDEN":
            return Response(
                {"error": "You are not allowed to access this tenant"}, status=403
            )
        if not tenant:
            return Response(
                {"error": f"Tenant '{tenant_id}' not found or user not assigned"},
                status=404,
            )

        table = self.resolve_ch_table(dataset_type, loan_type)

        ch_tenant_key = tenant.tenant_code.lower()
        ch = ClickHouseClient(tenant_schema=ch_tenant_key)

        res = ch.execute_query(f"SELECT * FROM {table} LIMIT 1000")
        return Response(
            {
                "tenant_id": tenant.tenant_code,
                "loan_type": loan_type,
                "dataset_type": dataset_type,
                "rows": res.result_rows,
            }
        )


class ProfilingViewSet(TenantResolverMixin, viewsets.ViewSet):
    """
    Get profiling stats.
    """

    permission_classes = [IsAuthenticated]

    def list(self, request):
        """
        Return profiling values.
        """
        tenant_id = (request.query_params.get("tenant_id") or "").upper()
        loan_type = (request.query_params.get("loan_type") or "").upper()
        dataset_type = (request.query_params.get("dataset_type") or "").upper()

        if not tenant_id or loan_type not in ["RETAIL", "COMMERCIAL"]:
            return Response(
                {"error": "tenant_id and valid loan_type required"}, status=400
            )
        if dataset_type not in ["CREDIT", "PAYMENT_PLAN"]:
            return Response({"error": "valid dataset_type required"}, status=400)

        tenant = self.resolve_tenant(request, tenant_id)
        if tenant == "FORBIDDEN":
            return Response(
                {"error": "You are not allowed to access this tenant"}, status=403
            )
        if not tenant:
            return Response(
                {"error": f"Tenant '{tenant_id}' not found or user not assigned"},
                status=404,
            )

        if dataset_type == "CREDIT":
            table = self.resolve_ch_table(dataset_type, loan_type)
            amount_col = "original_loan_amount"
        else:
            table = self.resolve_ch_table(dataset_type, loan_type)
            amount_col = "installment_amount"

        ch_tenant_key = tenant.tenant_code.lower()
        ch = ClickHouseClient(tenant_schema=ch_tenant_key)

        numeric_fields = get_numeric_fields(dataset_type)
        categorical_fields = get_categorical_fields(dataset_type)
        all_fields = get_field_names(dataset_type)

        # Total rows
        total_rows = ch.execute_query(f"SELECT count() FROM {table}").result_rows[0][0]

        null_ratio: dict[str, float] = {}
        if all_fields:
            null_exprs = [
                "count() as total_rows",
                *[
                    f"if(count()=0, 0, countIf({f} IS NULL) / count()) as {f}__null_ratio"
                    for f in all_fields
                ],
            ]
            null_res = ch.execute_query(f"SELECT {', '.join(null_exprs)} FROM {table}")
            null_row = null_res.result_rows[0]
            null_map = dict(zip(null_res.column_names, null_row))
            for f in all_fields:
                null_ratio[f] = float(null_map.get(f"{f}__null_ratio") or 0)

        numeric_stats: dict[str, dict] = {}
        if numeric_fields:
            num_exprs = [
                "count() as total_rows",
                *[
                    item
                    for f in numeric_fields
                    for item in (
                        f"min({f}) as {f}__min",
                        f"max({f}) as {f}__max",
                        f"avgOrNull({f}) as {f}__avg",
                        f"stddevPop({f}) as {f}__stddev",
                        f"if(count()=0, 0, countIf({f} IS NULL) / count()) as {f}__null_ratio",
                    )
                ],
            ]
            num_res = ch.execute_query(f"SELECT {', '.join(num_exprs)} FROM {table}")
            num_row = num_res.result_rows[0]
            num_map = dict(zip(num_res.column_names, num_row))
            for f in numeric_fields:
                numeric_stats[f] = {
                    "min": num_map.get(f"{f}__min"),
                    "max": num_map.get(f"{f}__max"),
                    "avg": num_map.get(f"{f}__avg"),
                    "stddev": num_map.get(f"{f}__stddev"),
                    "null_ratio": float(num_map.get(f"{f}__null_ratio") or 0),
                }

        categorical_stats: dict[str, dict] = {}
        if categorical_fields:
            cat_exprs = [
                "count() as total_rows",
                *[
                    item
                    for f in categorical_fields
                    for item in (
                        f"uniqExact({f}) as {f}__unique",
                        f"topK(1)({f}) as {f}__top",
                        f"if(count()=0, 0, countIf({f} IS NULL) / count()) as {f}__null_ratio",
                    )
                ],
            ]
            cat_res = ch.execute_query(f"SELECT {', '.join(cat_exprs)} FROM {table}")
            cat_row = cat_res.result_rows[0]
            cat_map = dict(zip(cat_res.column_names, cat_row))
            for f in categorical_fields:
                top_list = cat_map.get(f"{f}__top") or []
                most_freq = (
                    top_list[0]
                    if isinstance(top_list, (list, tuple)) and top_list
                    else None
                )
                categorical_stats[f] = {
                    "unique_count": cat_map.get(f"{f}__unique"),
                    "most_frequent": most_freq,
                    "null_ratio": float(cat_map.get(f"{f}__null_ratio") or 0),
                }

        amount_stats = numeric_stats.get(amount_col, {})

        return Response(
            {
                "tenant_id": tenant.tenant_code,
                "loan_type": loan_type,
                "dataset_type": dataset_type,
                "total_rows": total_rows,
                "numeric_stats": numeric_stats,
                "categorical_stats": categorical_stats,
                "null_ratio": null_ratio,
                # Backward compatible summary fields for UI cards
                "avg_amount": amount_stats.get("avg"),
                "min_amount": amount_stats.get("min"),
                "max_amount": amount_stats.get("max"),
                "stddev_amount": amount_stats.get("stddev"),
                "null_ratio_amount": amount_stats.get("null_ratio"),
            }
        )
