import os
from datetime import datetime, timezone
from typing import Any

try:
    from celery import Celery
    from celery.result import AsyncResult
except ImportError:  # pragma: no cover - exercised only when queue dependencies are missing
    Celery = None
    AsyncResult = None


DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_celery_app = None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_broker_url() -> str:
    return os.getenv("CELERY_BROKER_URL") or os.getenv("REDIS_URL") or DEFAULT_REDIS_URL


def _get_result_backend() -> str:
    return os.getenv("CELERY_RESULT_BACKEND") or _get_broker_url()


def get_celery_app():
    global _celery_app

    if Celery is None:
        raise RuntimeError(
            "Celery queue support is unavailable. Install the 'celery' and 'redis' packages first."
        )

    if _celery_app is None:
        celery_app = Celery(
            "skrejper",
            broker=_get_broker_url(),
            backend=_get_result_backend(),
            include=["app.tasks"],
        )
        celery_app.conf.update(
            accept_content=["json"],
            broker_connection_retry_on_startup=True,
            enable_utc=True,
            result_expires=int(os.getenv("CELERY_RESULT_EXPIRES_SECONDS", "3600")),
            result_serializer="json",
            task_serializer="json",
            task_track_started=True,
            timezone="UTC",
        )
        _celery_app = celery_app

    return _celery_app


def enqueue_task(task_name: str, **kwargs) -> dict[str, Any]:
    task = get_celery_app().send_task(task_name, kwargs=kwargs)
    return {
        "task_id": task.id,
        "task_name": task_name,
        "status": "queued",
        "queued_at": _utcnow_iso(),
    }


def get_task_status(task_id: str) -> dict[str, Any]:
    if AsyncResult is None:
        raise RuntimeError(
            "Celery queue support is unavailable. Install the 'celery' and 'redis' packages first."
        )

    task = AsyncResult(task_id, app=get_celery_app())
    payload = {
        "task_id": task_id,
        "status": str(task.status).lower(),
        "ready": task.ready(),
        "successful": task.successful(),
        "result": None,
        "error": None,
    }

    if task.successful():
        payload["result"] = task.result
    elif task.failed():
        payload["error"] = str(task.result)

    return payload
