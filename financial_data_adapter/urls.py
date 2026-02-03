"""Project URL routes."""

from django.contrib import admin
from django.contrib.auth.decorators import login_required
from django.urls import include, path
from django.views.generic import RedirectView, TemplateView

from .views import login_view, logout_view

urlpatterns = [
    path("admin/", admin.site.urls),
    path("login/", login_view, name="login"),
    path("logout/", logout_view, name="logout"),
    path("external-bank/", include("external_bank.urls")),
    path("api/", include("api.urls")),
    path(
        "dashboard/",
        login_required(
            TemplateView.as_view(template_name="dashboard.html"), login_url="/login/"
        ),
        name="dashboard",
    ),
    path(
        "simulation/",
        login_required(
            TemplateView.as_view(template_name="external_bank.html"),
            login_url="/login/",
        ),
        name="external_bank",
    ),
    path(
        "explorer/",
        login_required(
            TemplateView.as_view(template_name="data_explorer.html"),
            login_url="/login/",
        ),
        name="data_explorer",
    ),
    path("", RedirectView.as_view(url="/login/", permanent=False)),
]
