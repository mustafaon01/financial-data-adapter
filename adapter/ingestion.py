"""Ingestion service for adapter."""

import logging
import traceback
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.utils import timezone

from api.models import Batch, BatchError, Client, Loan, LoanPaymentPlan
from external_bank.models import DatasetState, MockLoan, MockLoanPaymentPlan

from .clickhouse_client import ClickHouseClient
from .schemas import get_clickhouse_columns, get_schema
from .validators import ErrorCodes, validate_and_normalize

logger = logging.getLogger(__name__)


class IngestionService:
    """Load data from external bank to storage."""

    def normalize_inputs(
        self, tenant_id: str, loan_type: str, dataset_type: str
    ) -> tuple[str, str, str]:
        """
        Clean and check input values.
        """
        tenant_id = (tenant_id or "").strip().upper()
        loan_type = (loan_type or "").strip().upper()
        dataset_type = (dataset_type or "CREDIT").strip().upper()

        if not tenant_id:
            raise ValueError("tenant_id is required")
        if loan_type not in ["RETAIL", "COMMERCIAL"]:
            raise ValueError("loan_type must be RETAIL or COMMERCIAL")
        if dataset_type not in ["CREDIT", "PAYMENT_PLAN"]:
            raise ValueError("dataset_type must be CREDIT or PAYMENT_PLAN")

        return tenant_id, loan_type, dataset_type

    def init_batch(self, tenant_id: str, loan_type: str, batch_id: int | None) -> Batch:
        """
        Create or load a batch and set status.
        """
        if batch_id:
            batch = Batch.objects.select_related("tenant").get(pk=batch_id)
        else:
            tenant = Client.objects.get(tenant_code__iexact=tenant_id)
            batch = Batch.objects.create(
                tenant=tenant,
                status=Batch.BatchStatus.STARTED,
                loan_type=loan_type,
                started_at=timezone.now(),
            )

        batch.status = Batch.BatchStatus.PROCESSING
        batch.started_at = batch.started_at or timezone.now()
        batch.loan_type = loan_type
        batch.save(update_fields=["status", "started_at", "loan_type", "updated_at"])
        return batch

    def get_source_queryset(self, bank_code: str, loan_type: str, dataset_type: str):
        """
        Load rows from external bank.
        """
        if dataset_type == "CREDIT":
            return (
                MockLoan.objects.filter(bank_code=bank_code, loan_type=loan_type)
                .order_by("id")
                .only("id", "payload", "external_id")
            )
        return (
            MockLoanPaymentPlan.objects.filter(bank_code=bank_code, loan_type=loan_type)
            .order_by("id")
            .only("id", "payload", "loan_external_id", "installment_number")
        )

    def get_credit_id_set(
        self, bank_code: str, loan_type: str, dataset_type: str
    ) -> set[str] | None:
        """
        Build credit id set for payment plans.
        """
        if dataset_type != "PAYMENT_PLAN":
            return None
        return set(
            MockLoan.objects.filter(
                bank_code=bank_code, loan_type=loan_type
            ).values_list("external_id", flat=True)
        )

    def validate_all_rows(
        self,
        qs,
        schema: list,
        dataset_type: str,
        credit_id_set: set | None,
        batch: Batch,
    ) -> tuple[int, int, list[BatchError]]:
        """
        Validate all rows. Return counts and errors.
        """
        errors_to_create: list[BatchError] = []
        valid_count = 0
        invalid_count = 0

        for row_num, item in enumerate(qs.iterator(chunk_size=2000), start=1):
            payload = item.payload or {}

            normalized, row_errors = self.normalize_row(payload, schema)
            if row_errors:
                invalid_count += 1
                for err in row_errors:
                    errors_to_create.append(
                        BatchError(
                            batch=batch,
                            row_number=row_num,
                            error_code=err["code"],
                            field_name=err.get("field"),
                            message=err["message"],
                            raw_excerpt=payload,
                        )
                    )
                continue

            raw_external_id = normalized.get("loan_account_number")
            if not self.is_payment_ref_valid(
                dataset_type, raw_external_id, credit_id_set
            ):
                invalid_count += 1
                errors_to_create.append(
                    BatchError(
                        batch=batch,
                        row_number=row_num,
                        error_code=ErrorCodes.UNKNOWN_LOAN_ID,
                        field_name="loan_account_number",
                        message="Payment plan references unknown loan_account_number.",
                        raw_excerpt=payload,
                    )
                )
                continue

            valid_count += 1

        return valid_count, invalid_count, errors_to_create

    def handle_validation_failure(
        self, batch: Batch, invalid_count: int, errors_to_create: list[BatchError]
    ) -> bool:
        """
        Save validation errors and close batch.
        """
        if errors_to_create:
            BatchError.objects.bulk_create(errors_to_create)
        batch.status = Batch.BatchStatus.FAILED_VALIDATION
        batch.error_message = f"Validation failed for {invalid_count} rows."
        batch.completed_at = timezone.now()
        batch.save(
            update_fields=[
                "status",
                "error_message",
                "completed_at",
                "updated_at",
            ]
        )
        return False

    def init_clickhouse(
        self, ch_tenant_key: str, dataset_type: str, loan_type: str, batch: Batch
    ):
        """
        Create staging table and return ClickHouse client.
        """
        init_client = ClickHouseClient()
        init_client.create_database(ch_tenant_key)
        ch = ClickHouseClient(tenant_schema=ch_tenant_key)

        target_table = self.resolve_target_table(dataset_type, loan_type)
        staging = f"{target_table}_staging_{str(batch.id).split('-')[0]}"
        order_by = self.resolve_order_by(dataset_type)

        ch_columns = get_clickhouse_columns(dataset_type)
        ch_columns.extend([("batch_id", "String"), ("loan_type", "String")])
        ddl_cols = ",\n".join([f"{name} {ctype}" for name, ctype in ch_columns])

        ch.execute_query(f"DROP TABLE IF EXISTS {staging}")
        ch.execute_query(
            f"""
            CREATE TABLE {staging} (
                {ddl_cols},
                created_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY {order_by}
            """
        )
        return ch, target_table, staging, [name for name, _ in ch_columns]

    def load_chunked(
        self,
        qs,
        schema: list,
        dataset_type: str,
        credit_id_set: set | None,
        tenant: Client,
        loan_type: str,
        batch: Batch,
        ch: ClickHouseClient,
        staging: str,
        cols: list[str],
    ) -> int:
        """
        Insert rows to Postgres and ClickHouse in chunks.
        """
        chunk_size = 2000
        record_count = 0
        ch_batch: list[dict[str, Any]] = []
        pg_batch: list[Loan] | list[LoanPaymentPlan] = []

        with transaction.atomic():
            if dataset_type == "CREDIT":
                Loan.objects.filter(tenant=tenant, loan_type=loan_type).delete()
            else:
                LoanPaymentPlan.objects.filter(
                    tenant=tenant, loan_type=loan_type
                ).delete()

            for row_num, item in enumerate(qs.iterator(chunk_size=chunk_size), start=1):
                payload = item.payload or {}
                normalized, row_errors = self.normalize_row(payload, schema)
                if row_errors:
                    continue
                raw_external_id = normalized.get("loan_account_number")
                if not self.is_payment_ref_valid(
                    dataset_type, raw_external_id, credit_id_set
                ):
                    continue

                record_count += 1

                if dataset_type == "CREDIT":
                    pg_batch.append(
                        Loan(
                            tenant=tenant,
                            external_id=str(raw_external_id),
                            loan_type=loan_type,
                            amount=normalized.get("original_loan_amount")
                            or Decimal("0"),
                            interest_rate=normalized.get("nominal_interest_rate")
                            or Decimal("0"),
                            customer_name=(normalized.get("customer_id") or "Customer"),
                            is_active=(normalized.get("loan_status_code") == "ACTIVE"),
                        )
                    )
                elif dataset_type == "PAYMENT_PLAN":
                    pg_batch.append(
                        LoanPaymentPlan(
                            tenant=tenant,
                            loan_type=loan_type,
                            loan_external_id=str(raw_external_id),
                            installment_number=normalized.get("installment_number"),
                            scheduled_payment_date=normalized.get(
                                "scheduled_payment_date"
                            ),
                            installment_amount=normalized.get("installment_amount"),
                            payload=payload,
                        )
                    )

                ch_batch.append(
                    {
                        **normalized,
                        "batch_id": str(batch.id),
                        "loan_type": loan_type,
                    }
                )

                if len(ch_batch) >= chunk_size:
                    data = [[r[c] for c in cols] for r in ch_batch]
                    ch.insert_data(staging, data, column_names=cols)
                    ch_batch.clear()

                if len(pg_batch) >= chunk_size:
                    if dataset_type == "CREDIT":
                        Loan.objects.bulk_create(pg_batch)
                    else:
                        LoanPaymentPlan.objects.bulk_create(pg_batch)
                    pg_batch.clear()

            if ch_batch:
                data = [[r[c] for c in cols] for r in ch_batch]
                ch.insert_data(staging, data, column_names=cols)
                ch_batch.clear()

            if pg_batch:
                if dataset_type == "CREDIT":
                    Loan.objects.bulk_create(pg_batch)
                else:
                    LoanPaymentPlan.objects.bulk_create(pg_batch)
                pg_batch.clear()

        return record_count

    def finalize_batch_success(self, batch: Batch, record_count: int) -> bool:
        """
        Mark batch as success.
        """
        batch.status = Batch.BatchStatus.SUCCESS
        batch.record_count = record_count
        batch.completed_at = timezone.now()
        batch.save(
            update_fields=["status", "record_count", "completed_at", "updated_at"]
        )
        return True

    def normalize_row(self, payload: dict, schema: list) -> tuple[dict, list[dict]]:
        """
        Validate and normalize one row.
        """
        return validate_and_normalize(payload, schema)

    def is_payment_ref_valid(
        self, dataset_type: str, raw_external_id: str | None, credit_id_set: set | None
    ) -> bool:
        """
        Check payment plan reference.
        """
        if dataset_type != "PAYMENT_PLAN":
            return True
        if not raw_external_id:
            return False
        return raw_external_id in (credit_id_set or set())

    @staticmethod
    def resolve_target_table(dataset_type: str, loan_type: str) -> str:
        """Build target table name."""
        dataset_type = (dataset_type or "").upper()
        loan_type = (loan_type or "").upper()
        prefix = (
            "fact_loans_current"
            if dataset_type == "CREDIT"
            else "fact_payment_plan_current"
        )
        suffix = "retail" if loan_type == "RETAIL" else "commercial"
        return f"{prefix}_{suffix}"

    @staticmethod
    def resolve_order_by(dataset_type: str) -> str:
        """Build order by clause."""
        if (dataset_type or "").upper() == "CREDIT":
            return "(loan_type, loan_account_number)"
        return "(loan_type, loan_account_number, installment_number)"

    @staticmethod
    def bulk_replace_loans(tenant: Client, loan_type: str, loans: list[Loan]) -> None:
        """Replace loan rows for tenant."""
        uniq: dict[str, Loan] = {}
        for loan in loans:
            uniq[loan.external_id] = loan
        deduped = list(uniq.values())
        if not deduped:
            return
        Loan.objects.filter(
            tenant=tenant,
            loan_type=loan_type,
            external_id__in=list(uniq.keys()),
        ).delete()
        Loan.objects.bulk_create(deduped)

    @staticmethod
    def bulk_replace_plans(
        tenant: Client, loan_type: str, plans: list[LoanPaymentPlan]
    ) -> None:
        """Replace payment plan rows for tenant."""
        uniq: dict[tuple[str, int], LoanPaymentPlan] = {}
        for plan in plans:
            uniq[(plan.loan_external_id, plan.installment_number)] = plan
        deduped = list(uniq.values())
        if not deduped:
            return
        loan_ids = list({k[0] for k in uniq.keys()})
        LoanPaymentPlan.objects.filter(
            tenant=tenant,
            loan_type=loan_type,
            loan_external_id__in=loan_ids,
        ).delete()
        LoanPaymentPlan.objects.bulk_create(deduped)

    def run_ingestion(
        self,
        tenant_id: str,
        loan_type: str,
        dataset_type: str = "CREDIT",
        batch_id: int | None = None,
    ) -> bool:
        """
        Run validation, normalize, and load data.
        """
        tenant_id, loan_type, dataset_type = self.normalize_inputs(
            tenant_id, loan_type, dataset_type
        )
        batch = self.init_batch(tenant_id, loan_type, batch_id)

        try:
            tenant = batch.tenant
            bank_code = tenant.tenant_code.upper()  # BANK001
            ch_tenant_key = tenant.tenant_code.lower()  # bank001

            state = DatasetState.objects.filter(
                bank_code=bank_code, loan_type=loan_type, dataset_type=dataset_type
            ).first()
            current_ver = state.dataset_version if state else None
            logger.info(
                f"[INGEST] tenant={bank_code} loan_type={loan_type} dataset_type={dataset_type} ver={current_ver}"
            )

            qs = self.get_source_queryset(bank_code, loan_type, dataset_type)

            total_rows = qs.count()
            batch.total_rows = total_rows
            batch.save(update_fields=["total_rows", "updated_at"])

            if total_rows == 0:
                batch.status = Batch.BatchStatus.SUCCESS
                batch.completed_at = timezone.now()
                batch.record_count = 0
                batch.valid_rows = 0
                batch.invalid_rows = 0
                batch.save(
                    update_fields=[
                        "status",
                        "completed_at",
                        "record_count",
                        "valid_rows",
                        "invalid_rows",
                        "updated_at",
                    ]
                )
                return True

            schema = get_schema(dataset_type)
            credit_id_set = self.get_credit_id_set(bank_code, loan_type, dataset_type)

            valid_count, invalid_count, errors_to_create = self.validate_all_rows(
                qs, schema, dataset_type, credit_id_set, batch
            )

            batch.valid_rows = valid_count
            batch.invalid_rows = invalid_count
            batch.save(update_fields=["valid_rows", "invalid_rows", "updated_at"])

            if invalid_count > 0:
                return self.handle_validation_failure(
                    batch, invalid_count, errors_to_create
                )

            ch, target_table, staging, cols = self.init_clickhouse(
                ch_tenant_key, dataset_type, loan_type, batch
            )
            record_count = self.load_chunked(
                qs=qs,
                schema=schema,
                dataset_type=dataset_type,
                credit_id_set=credit_id_set,
                tenant=tenant,
                loan_type=loan_type,
                batch=batch,
                ch=ch,
                staging=staging,
                cols=cols,
            )
            ch.execute_query(f"CREATE TABLE IF NOT EXISTS {target_table} AS {staging}")
            ch.swap_tables(target_table, staging)
            ch.execute_query(f"DROP TABLE IF EXISTS {staging}")

            return self.finalize_batch_success(batch, record_count)

        except Exception as e:
            logger.error(f"Ingestion crashed: {e}")
            traceback.print_exc()
            batch.status = Batch.BatchStatus.FAILED
            batch.error_message = f"System Error: {str(e)}"
            batch.completed_at = timezone.now()
            batch.save(
                update_fields=["status", "error_message", "completed_at", "updated_at"]
            )
            raise
