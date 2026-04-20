# tasks.py
"""
Celery tasks for the webhook service.

Register this task in redbeat via:

    from redbeat_s.red_functions import create_redbeat_schedule
    from executors import celery_app

    create_redbeat_schedule(
        schedule_name   = "webhook_retry_task",
        executor        = "redbeat_s.tasks.retry_webhooks_task",
        schedule_minutes= 1,        # runs every minute
        celery_app      = celery_app
    )
"""

import logging
from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name="redbeat_s.tasks.retry_webhooks_task", bind=True, max_retries=0)
def retry_webhooks_task(self):
    """
    Celery Beat task — retries all FAILED webhook deliveries
    whose next_retry_date is due.
    """
    try:
        from utils.webhook_service import retry_failed_deliveries
        retry_failed_deliveries()
    except Exception as exc:
        logger.error(f"[Webhook Retry Task] Unhandled error: {exc}", exc_info=True)
