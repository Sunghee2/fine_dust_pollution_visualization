from django.urls import path, include
from .views import handle_webhook
from django.views.decorators.csrf import csrf_exempt

urlpatterns = [
    path('push', csrf_exempt(handle_webhook))
]
