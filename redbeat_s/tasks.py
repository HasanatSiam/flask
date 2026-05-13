# tasks.py
"""
Celery tasks for the webhook service.

Per-delivery retries are scheduled with ``eta`` when a delivery fails; see
``utils.webhook_service.schedule_webhook_retry_task``.

The periodic ``retry_webhooks_task`` is a rare sweeper for stragglers (lost ETA
messages). For RedBeat-only registration examples, see docstrings below.
"""

import logging
from celery import shared_task
from utils.webhook_service import sweep_overdue_webhook_retries, retry_one_failed_delivery

logger = logging.getLogger(__name__)


@shared_task(
    name="redbeat_s.tasks.retry_single_webhook_delivery",
    bind=True,
    max_retries=0,
)
def retry_single_webhook_delivery(self, delivery_id: int, sweeper_mode: bool = False):
    """
    Retry one FAILED ``LogWebhookDelivery`` row (ETA path or sweeper path).

    Idempotency and eligibility checks live in ``retry_one_failed_delivery``.
    """
    try:
        retry_one_failed_delivery(delivery_id, sweeper_mode=sweeper_mode)
    except Exception as exc:
        logger.error(
            f"[Webhook retry_single] delivery_id={delivery_id}: {exc}",
            exc_info=True,
        )


@shared_task(name="redbeat_s.tasks.retry_webhooks_task", bind=True, max_retries=0)
def retry_webhooks_task(self):
    """
    Celery Beat sweeper — processes FAILED deliveries that are far past
    ``next_retry_date`` (ETA task likely never ran).
    """
    try:
        sweep_overdue_webhook_retries()
    except Exception as exc:
        logger.error(f"[Webhook sweeper task] Unhandled error: {exc}", exc_info=True)
