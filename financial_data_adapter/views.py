"""Views for login and logout."""

from django.contrib.auth import authenticate, login, logout
from django.shortcuts import redirect, render

from api.models import Client, UserTenant


def login_view(request):
    """
    GET/POST /login/

    Login with username/password and select tenant for superusers.
    Normal users are restricted to their assigned tenant.
    """
    if request.user.is_authenticated:
        return redirect("dashboard")

    error = None
    locked_tenant = None
    tenant_options = Client.objects.order_by("tenant_code")

    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        tenant_code = (request.POST.get("tenant_id") or "").strip().upper()

        user = authenticate(request, username=username, password=password)
        if not user:
            error = "Invalid username or password."
        else:
            if user.is_superuser:
                if not tenant_code:
                    error = "Tenant selection is required for superuser."
                else:
                    tenant = Client.objects.filter(
                        tenant_code__iexact=tenant_code
                    ).first()
                    if not tenant:
                        error = "Selected tenant not found."
                    else:
                        login(request, user)
                        request.session["active_tenant"] = tenant.tenant_code
                        return redirect("dashboard")
            else:
                try:
                    link = UserTenant.objects.select_related("tenant").get(user=user)
                except UserTenant.DoesNotExist:
                    error = "User is not assigned to a tenant."
                else:
                    assigned_code = link.tenant.tenant_code.upper()
                    if tenant_code and tenant_code != assigned_code:
                        error = (
                            "You are not allowed to use this tenant. "
                            f"Assigned tenant: {assigned_code}"
                        )
                        locked_tenant = assigned_code
                    else:
                        login(request, user)
                        request.session["active_tenant"] = assigned_code
                        return redirect("dashboard")

    return render(
        request,
        "login.html",
        {
            "error": error,
            "tenant_options": tenant_options,
            "locked_tenant": locked_tenant,
        },
    )


def logout_view(request):
    """
    GET/POST /logout/
    """
    if request.user.is_authenticated:
        logout(request)
    return redirect("login")
