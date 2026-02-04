"""External bank API views."""

import csv
import hashlib
import io
import logging
import uuid

from django.db import transaction
from django.utils import timezone
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.models import UserTenant

from .csv_read_helpers import (
    detect_external_id,
    normalize_bank_code,
    normalize_loan_type,
    safe_str,
    sniff_dialect,
)
from .models import DatasetState, MockLoan, MockLoanPaymentPlan

logger = logging.getLogger(__name__)

ALLOWED_LOAN_TYPES = {"RETAIL", "COMMERCIAL"}
ALLOWED_DATASET_TYPES = {"CREDIT", "PAYMENT_PLAN"}


def detect_dataset_type_from_headers(fieldnames: list[str]) -> str:
    """
    Detect dataset type by headers.
    """
    if not fieldnames:
        return "CREDIT"

    headers = {h.strip().lower() for h in fieldnames if h}
    if "installment_number" in headers or "scheduled_payment_date" in headers:
        return "PAYMENT_PLAN"
    return "CREDIT"


class CSVUploadView(APIView):
    """
    Upload CSV and update mock data.
    """

    parser_classes = [MultiPartParser]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        POST /external-bank/upload-csv/
        """
        bank_code = normalize_bank_code(request.data.get("bank_code"))
        loan_type = normalize_loan_type(request.data.get("loan_type"))
        dataset_type = (request.data.get("dataset_type") or "").strip().upper()
        file_obj = request.FILES.get("file")

        error = self.validate_request(request, bank_code, loan_type, file_obj)
        if error:
            return error

        dataset_type = self.resolve_dataset_type(dataset_type, file_obj)
        if isinstance(dataset_type, Response):
            return dataset_type

        result = self.process_upload(
            bank_code=bank_code,
            loan_type=loan_type,
            dataset_type=dataset_type,
            file_obj=file_obj,
        )
        return Response(result)

    def validate_request(self, request, bank_code, loan_type, file_obj):
        """
        Check request fields.
        """
        if not bank_code:
            return Response({"error": "bank_code is required"}, status=400)

        tenant_error = self.check_tenant_access(request, bank_code)
        if tenant_error:
            return tenant_error

        if not loan_type:
            return Response({"error": "loan_type is required"}, status=400)
        if loan_type not in ALLOWED_LOAN_TYPES:
            return Response(
                {"error": f"loan_type must be one of {sorted(ALLOWED_LOAN_TYPES)}"},
                status=400,
            )
        if not file_obj:
            return Response({"error": "No file uploaded"}, status=400)
        return None

    def check_tenant_access(self, request, bank_code):
        """
        Allow only own tenant or superuser.
        """
        if request.user.is_superuser:
            return None
        try:
            link = UserTenant.objects.select_related("tenant").get(user=request.user)
        except UserTenant.DoesNotExist:
            return Response({"error": "User is not assigned to a tenant"}, status=403)
        if link.tenant.tenant_code.upper() != bank_code:
            return Response(
                {"error": "You are not allowed to upload for this tenant"},
                status=403,
            )
        return None

    def resolve_dataset_type(self, dataset_type, file_obj):
        """
        Detect dataset type when missing.
        """
        raw = file_obj.file.read(2048)
        file_obj.file.seek(0)
        sample = raw.decode("utf-8", errors="replace")
        dialect = sniff_dialect(sample)
        reader = csv.DictReader(io.StringIO(sample), dialect=dialect)

        if not dataset_type:
            dataset_type = detect_dataset_type_from_headers(reader.fieldnames or [])
        if dataset_type not in ALLOWED_DATASET_TYPES:
            return Response(
                {
                    "error": f"dataset_type must be one of {sorted(ALLOWED_DATASET_TYPES)}"
                },
                status=400,
            )
        return dataset_type

    def process_upload(self, bank_code, loan_type, dataset_type, file_obj):
        """
        Parse CSV and save to DB.
        """
        new_version = uuid.uuid4()
        hasher = hashlib.sha256()

        wrapper = io.TextIOWrapper(
            file_obj.file, encoding="utf-8", errors="replace", newline=""
        )
        sample = wrapper.read(2048)
        wrapper.seek(0)
        dialect = sniff_dialect(sample)
        reader = csv.DictReader(wrapper, dialect=dialect)

        chunk_size = 2000
        upserted_total = 0
        skipped_total = 0

        started_at = timezone.now()

        with transaction.atomic():
            if dataset_type == "CREDIT":
                upserted_total, skipped_total = self.process_credit_csv(
                    reader=reader,
                    bank_code=bank_code,
                    loan_type=loan_type,
                    dataset_version=new_version,
                    hasher=hasher,
                    chunk_size=chunk_size,
                )
            else:
                upserted_total, skipped_total = self.process_payment_plan_csv(
                    reader=reader,
                    bank_code=bank_code,
                    loan_type=loan_type,
                    dataset_version=new_version,
                    hasher=hasher,
                    chunk_size=chunk_size,
                )

            checksum = hasher.hexdigest()

            state, _ = DatasetState.objects.update_or_create(
                bank_code=bank_code,
                loan_type=loan_type,
                dataset_type=dataset_type,
                defaults={
                    "dataset_version": new_version,
                    "checksum": checksum,
                },
            )

        duration_ms = int((timezone.now() - started_at).total_seconds() * 1000)

        return {
            "status": "success",
            "bank_code": bank_code,
            "loan_type": loan_type,
            "dataset_type": dataset_type,
            "dataset_version": str(state.dataset_version),
            "checksum": state.checksum,
            "processed_rows": upserted_total,
            "skipped_rows": skipped_total,
            "duration_ms": duration_ms,
        }

    @staticmethod
    def process_credit_csv(
        reader: csv.DictReader,
        bank_code: str,
        loan_type: str,
        dataset_version: uuid.UUID,
        hasher: "hashlib._Hash",
        chunk_size: int,
    ) -> tuple[int, int]:
        chunk_map: dict[tuple[str, str, str], MockLoan] = {}
        upserted_total = 0
        skipped_total = 0

        for _, row in enumerate(reader, start=1):
            try:
                hasher.update(str(row).encode("utf-8", errors="ignore"))
            except Exception:
                pass

            ext_id = detect_external_id(row)
            if not ext_id:
                skipped_total += 1
                continue

            cust_id = safe_str(row.get("customer_id"))
            key = (bank_code, loan_type, ext_id)

            chunk_map[key] = MockLoan(
                bank_code=bank_code,
                loan_type=loan_type,
                external_id=ext_id,
                customer_id=cust_id,
                payload=row,
                dataset_version=dataset_version,
            )

            if len(chunk_map) >= chunk_size:
                CSVUploadView.bulk_upsert_credit(list(chunk_map.values()))
                upserted_total += len(chunk_map)
                chunk_map.clear()

        if chunk_map:
            CSVUploadView.bulk_upsert_credit(list(chunk_map.values()))
            upserted_total += len(chunk_map)
            chunk_map.clear()

        return upserted_total, skipped_total

    @staticmethod
    def process_payment_plan_csv(
        reader: csv.DictReader,
        bank_code: str,
        loan_type: str,
        dataset_version: uuid.UUID,
        hasher: "hashlib._Hash",
        chunk_size: int,
    ) -> tuple[int, int]:
        chunk_map: dict[tuple[str, str, str, int], MockLoanPaymentPlan] = {}
        upserted_total = 0
        skipped_total = 0

        for _, row in enumerate(reader, start=1):
            try:
                hasher.update(str(row).encode("utf-8", errors="ignore"))
            except Exception:
                pass

            loan_ext_id = detect_external_id(row)
            if not loan_ext_id:
                skipped_total += 1
                continue

            inst_raw = row.get("installment_number")
            try:
                inst_no = (
                    int(str(inst_raw).strip()) if inst_raw not in (None, "") else None
                )
            except (TypeError, ValueError):
                inst_no = None

            if inst_no is None:
                skipped_total += 1
                continue

            cust_id = safe_str(row.get("customer_id"))
            key = (bank_code, loan_type, loan_ext_id, inst_no)

            chunk_map[key] = MockLoanPaymentPlan(
                bank_code=bank_code,
                loan_type=loan_type,
                loan_external_id=loan_ext_id,
                installment_number=inst_no,
                customer_id=cust_id,
                payload=row,
                dataset_version=dataset_version,
            )

            if len(chunk_map) >= chunk_size:
                CSVUploadView.bulk_upsert_payment_plan(list(chunk_map.values()))
                upserted_total += len(chunk_map)
                chunk_map.clear()

        if chunk_map:
            CSVUploadView.bulk_upsert_payment_plan(list(chunk_map.values()))
            upserted_total += len(chunk_map)
            chunk_map.clear()

        return upserted_total, skipped_total

    @staticmethod
    def bulk_upsert_credit(objs: list[MockLoan]) -> None:
        MockLoan.objects.bulk_create(
            objs,
            update_conflicts=True,
            unique_fields=["bank_code", "loan_type", "external_id"],
            update_fields=["payload", "customer_id", "dataset_version", "updated_at"],
        )

    @staticmethod
    def bulk_upsert_payment_plan(objs: list[MockLoanPaymentPlan]) -> None:
        MockLoanPaymentPlan.objects.bulk_create(
            objs,
            update_conflicts=True,
            unique_fields=[
                "bank_code",
                "loan_type",
                "loan_external_id",
                "installment_number",
            ],
            update_fields=["payload", "customer_id", "dataset_version", "updated_at"],
        )


def get_request_body(request):
    """
    Common request handler method
    """
    bank_code = normalize_bank_code(request.query_params.get("bank_code"))
    loan_type = normalize_loan_type(request.query_params.get("loan_type"))
    dataset_type = (request.query_params.get("dataset_type") or "").strip().upper()

    if not bank_code or not loan_type:
        return Response({"error": "bank_code and loan_type are required"}, status=400)
    if not dataset_type:
        return Response({"error": "dataset_type is required"}, status=400)
    if dataset_type not in ALLOWED_DATASET_TYPES:
        return Response(
            {"error": f"dataset_type must be one of {sorted(ALLOWED_DATASET_TYPES)}"},
            status=400,
        )
    return bank_code, loan_type, dataset_type


class VersionView(APIView):
    """
    Return dataset version.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        GET /external-bank/version/
        """

        bank_code, loan_type, dataset_type = get_request_body(request)

        state = DatasetState.objects.filter(
            bank_code=bank_code, loan_type=loan_type, dataset_type=dataset_type
        ).first()
        if not state:
            return Response(
                {"dataset_version": None, "checksum": None, "updated_at": None}
            )

        return Response(
            {
                "bank_code": bank_code,
                "loan_type": loan_type,
                "dataset_type": dataset_type,
                "dataset_version": str(state.dataset_version),
                "checksum": state.checksum,
                "updated_at": state.updated_at,
            }
        )


class CurrentDataView(APIView):
    """
    Return current dataset rows.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        GET /external-bank/current/
        """
        bank_code, loan_type, dataset_type = get_request_body(request)

        try:
            limit = int(request.query_params.get("limit", 1000))
        except ValueError:
            return Response({"error": "limit must be an integer"}, status=400)

        if limit < 1:
            limit = 1
        if limit > 10000:
            limit = 10000

        try:
            cursor = int(request.query_params.get("cursor", 0))
        except ValueError:
            return Response({"error": "cursor must be an integer"}, status=400)

        state = DatasetState.objects.filter(
            bank_code=bank_code, loan_type=loan_type, dataset_type=dataset_type
        ).first()
        current_ver = str(state.dataset_version) if state else None

        if dataset_type == "CREDIT":
            qs = (
                MockLoan.objects.filter(
                    bank_code=bank_code, loan_type=loan_type, id__gt=cursor
                )
                .order_by("id")
                .only("id", "payload")[:limit]
            )
        else:
            qs = (
                MockLoanPaymentPlan.objects.filter(
                    bank_code=bank_code, loan_type=loan_type, id__gt=cursor
                )
                .order_by("id")
                .only("id", "payload")[:limit]
            )

        data = []
        last_id = None
        for item in qs:
            data.append(item.payload)
            last_id = item.id

        next_cursor = last_id if (last_id is not None and len(data) == limit) else None

        return Response(
            {
                "bank_code": bank_code,
                "loan_type": loan_type,
                "dataset_type": dataset_type,
                "dataset_version": current_ver,
                "next_cursor": next_cursor,
                "data": data,
            }
        )
