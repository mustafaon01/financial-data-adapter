import base64
from django_tenants.middleware.main import TenantMainMiddleware
from django_tenants.utils import get_public_schema_name


class BasicAuthTenantMiddleware(TenantMainMiddleware):
    """
    Middleware that identifies the tenant based on the Basic Authentication username.

    This implementation overrides the default domain-based tenant discovery to support
    a single-domain architecture. It decodes the 'Authorization' header and uses the
    username as the schema identifier.
    """

    def get_tenant(self, model, hostname, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION")

        if auth_header and auth_header.startswith("Basic "):
            try:
                auth_decoded = base64.b64decode(auth_header.split(" ")[1]).decode(
                    "utf-8"
                )
                username = auth_decoded.split(":")[0]
                schema_name = username.lower()

                return model.objects.get(schema_name=schema_name)
            except (IndexError, model.DoesNotExist, Exception):
                pass

        return model.objects.get(schema_name=get_public_schema_name())
