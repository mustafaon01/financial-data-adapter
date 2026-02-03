"""API route configuration."""

from rest_framework.routers import DefaultRouter

from .views import BatchViewSet, DataViewSet, ProfilingViewSet, SyncViewSet

router = DefaultRouter()
router.register(r"batches", BatchViewSet, basename="batches")
router.register(r"sync", SyncViewSet, basename="sync")
router.register(r"data", DataViewSet, basename="data")
router.register(r"profiling", ProfilingViewSet, basename="profiling")

urlpatterns = router.urls
