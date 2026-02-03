import csv
import hashlib
import io
import logging
import uuid
from typing import Dict, Tuple, List

from django.db import transaction
from django.utils import timezone

from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import MockLoan, DatasetState
from .csv_read_helpers import normalize_bank_code, normalize_loan_type, sniff_dialect, detect_external_id, safe_str

logger = logging.getLogger(__name__)

ALLOWED_LOAN_TYPES = {"RETAIL", "COMMERCIAL"}

class CSVUploadView(APIView):
    """
    Handle CSV uploads to update the mock bank data.

    It parses the file, updates the loans (Upsert), and saving a new dataset version.
    """

    parser_classes = [MultiPartParser]
    permission_classes = [AllowAny]

    def post(self, request):
        """
        Process the uploaded CSV file.

        :param request: The HTTP request containing the file and metadata.
        :return: A JSON response with the processing result.
        """
        bank_code = normalize_bank_code(request.data.get("bank_code"))
        loan_type = normalize_loan_type(request.data.get("loan_type"))
        file_obj = request.FILES.get("file")

        if not bank_code:
            return Response({"error": "bank_code is required"}, status=400)
        if not loan_type:
            return Response({"error": "loan_type is required"}, status=400)
        if loan_type not in ALLOWED_LOAN_TYPES:
            return Response({"error": f"loan_type must be one of {sorted(ALLOWED_LOAN_TYPES)}"}, status=400)
        if not file_obj:
            return Response({"error": "No file uploaded"}, status=400)

        new_version = uuid.uuid4()
        hasher = hashlib.sha256()

        wrapper = io.TextIOWrapper(file_obj.file, encoding="utf-8", errors="replace", newline="")

        sample = wrapper.read(2048)
        wrapper.seek(0)
        dialect = sniff_dialect(sample)

        reader = csv.DictReader(wrapper, dialect=dialect)

        chunk_size = 2000
        upserted_total = 0
        skipped_total = 0

        chunk_map: Dict[Tuple[str, str, str], MockLoan] = {}

        started_at = timezone.now()

        with transaction.atomic():
            for row_idx, row in enumerate(reader, start=1):
                try:
                    hasher.update(str(row).encode("utf-8", errors="ignore"))
                except Exception:
                    # checksum is best-effort; do not fail upload due to hashing
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
                    dataset_version=new_version
                )

                if len(chunk_map) >= chunk_size:
                    self.bulk_upsert(list(chunk_map.values()))
                    upserted_total += len(chunk_map)
                    chunk_map.clear()

            if chunk_map:
                self.bulk_upsert(list(chunk_map.values()))
                upserted_total += len(chunk_map)
                chunk_map.clear()

            checksum = hasher.hexdigest()

            state, _ = DatasetState.objects.update_or_create(
                bank_code=bank_code,
                loan_type=loan_type,
                defaults={
                    "dataset_version": new_version,
                    "checksum": checksum,
                },
            )

        duration_ms = int((timezone.now() - started_at).total_seconds() * 1000)

        return Response(
            {
                "status": "success",
                "bank_code": bank_code,
                "loan_type": loan_type,
                "dataset_version": str(state.dataset_version),
                "checksum": state.checksum,
                "processed_rows": upserted_total,
                "skipped_rows": skipped_total,
                "duration_ms": duration_ms,
            }
        )

    @staticmethod
    def bulk_upsert(objs: List[MockLoan]) -> None:
        """
        Insert or update a list of MockLoan objects.

        This uses PostgreSQL's ON CONFLICT feature to handle duplicates.

        :param objs: A list of MockLoan objects to save.
        """

        MockLoan.objects.bulk_create(
            objs,
            update_conflicts=True,
            unique_fields=["bank_code", "loan_type", "external_id"],
            update_fields=["payload", "customer_id", "updated_at"],
        )


class VersionView(APIView):
    """
    Provide the current version of the dataset.

    This handles GET requests to check if new data is available.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        """
        Retrieve the latest dataset version.

        :param request: The HTTP request with query parameters.
        :return: JSON with version and checksum, or empty values if not found.
        """
        bank_code = normalize_bank_code(request.query_params.get("bank_code"))
        loan_type = normalize_loan_type(request.query_params.get("loan_type"))

        if not bank_code or not loan_type:
            return Response({"error": "bank_code and loan_type are required"}, status=400)

        state = DatasetState.objects.filter(bank_code=bank_code, loan_type=loan_type).first()
        if not state:
            return Response({"dataset_version": None, "checksum": None, "updated_at": None})

        return Response(
            {
                "bank_code": bank_code,
                "loan_type": loan_type,
                "dataset_version": str(state.dataset_version),
                "checksum": state.checksum,
                "updated_at": state.updated_at,
            }
        )


class CurrentDataView(APIView):
    """
    Stream the current bank data.

    This endpoint uses cursor-based pagination to serve large datasets efficiently.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        """
        Fetch a page of loan data.

        :param request: The HTTP request with pagination params (cursor, limit).
        :return: JSON with a list of loans and the next cursor.
        """
        bank_code = normalize_bank_code(request.query_params.get("bank_code"))
        loan_type = normalize_loan_type(request.query_params.get("loan_type"))

        if not bank_code or not loan_type:
            return Response({"error": "bank_code and loan_type are required"}, status=400)

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

        state = DatasetState.objects.filter(bank_code=bank_code, loan_type=loan_type).first()
        current_ver = str(state.dataset_version) if state else None

        qs = (
            MockLoan.objects.filter(bank_code=bank_code, loan_type=loan_type, id__gt=cursor)
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
                "dataset_version": current_ver,
                "next_cursor": next_cursor,
                "data": data,
            }
        )
