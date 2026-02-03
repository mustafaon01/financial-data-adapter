from django.urls import path
from .views import CSVUploadView, VersionView, CurrentDataView

urlpatterns = [
    path("upload-csv/", CSVUploadView.as_view()),
    path("version/", VersionView.as_view()),
    path("current/", CurrentDataView.as_view()),
]
