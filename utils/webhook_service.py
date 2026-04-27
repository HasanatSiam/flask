# utils/webhook_service.py
"""
Service: Endpoint-Centric Webhook Service
=====================================================
This service dispatches webhooks based on specific API actions (Endpoint IDs).
It provides a standardized envelope.
"""

import hmac
import hashlib
import json
import logging
import requests
import time
from datetime import datetime
from sqlalchemy.orm import sessionmaker

from executors.extensions import db
from executors.models import (
    DefWebhook,
    DefWebhookSubscription,
    LogWebhookDelivery,
    DefWebhookEvent,
)

logger = logging.getLogger(__name__)

# ── Backoff schedule (seconds) per attempt number ────────────────────────────
RETRY_DELAYS = {
    1: 60,    # retry 1 -> 1 min
    2: 300,   # retry 2 -> 5 min
    3: 900,   # retry 3 -> 15 min
    4: 1800,  # retry 4 -> 30 min
    5: 3600,  # retry 5 -> 1 hr
}


def _sign_payload(secret_key: str, body: bytes) -> str:
    """Return HMAC-SHA256 hex digest signature for the raw payload bytes."""
    return hmac.new(secret_key.encode(), body, hashlib.sha256).hexdigest()


def _apply_filters(filters: dict | None, payload: dict) -> bool:
    """Return True if the payload passes the filters."""
    if not filters:
        return True
    for key, expected_value in filters.items():
        if str(payload.get(key)) != str(expected_value):
            return False
    return True


def _shape_payload(data: dict, selected_columns: list | None) -> dict:
    """Return a subset of the data based on selected_columns (root keys)."""
    if not selected_columns or not isinstance(selected_columns, list):
        return data
    return {k: v for k, v in data.items() if k in selected_columns}


def _dispatch(
    webhook: DefWebhook, delivery: LogWebhookDelivery, payload_bytes: bytes
) -> None:
    """Perform the actual HTTP call and update delivery log."""
    headers = {"Content-Type": "application/json"}

    # Add custom extra headers
    if webhook.extra_headers:
        headers.update(webhook.extra_headers)

    if webhook.secret_key:
        signature = _sign_payload(webhook.secret_key, payload_bytes)
        headers["X-PROCG-Signature"] = f"sha256={signature}"

    start = time.time()
    try:
        response = requests.post(
            url=webhook.webhook_url,
            data=payload_bytes,
            headers=headers,
            timeout=10,
        )
        duration_ms = int((time.time() - start) * 1000)

        delivery.http_status = response.status_code
        delivery.response_body = response.text[:4000]
        delivery.duration_ms = duration_ms
        delivery.delivery_status = (
            "SUCCESS" if 200 <= response.status_code < 300 else "FAILED"
        )

    except Exception as exc:
        delivery.delivery_status = "FAILED"
        delivery.response_body = str(exc)[:500]
        delivery.duration_ms = int((time.time() - start) * 1000)


def fire(api_endpoint_id: int, payload: dict, tenant_id: int) -> None:
    """
    Finds all business events and active webhooks for a given endpoint and dispatches them.
    Supports filters and data shaping (selected columns).
    """
    Session = sessionmaker(bind=db.engine)
    session = Session()

    try:
        # 1. Find all Business Events associated with this technical endpoint
        events = (
            session.query(DefWebhookEvent)
            .filter_by(api_endpoint_id=api_endpoint_id)
            .all()
        )
        if not events:
            return

        for event in events:
            # 2. Find all active webhooks subscribed to this specific business event for this tenant
            subscriptions = (
                session.query(DefWebhookSubscription, DefWebhook)
                .join(
                    DefWebhook,
                    DefWebhookSubscription.webhook_id
                    == DefWebhook.webhook_id,
                )
                .filter(
                    DefWebhookSubscription.event_id == event.event_id,
                    DefWebhook.tenant_id == tenant_id,
                    DefWebhook.is_active == "Y",
                )
                .all()
            )

            for sub, webhook in subscriptions:
                # Use a separate session for each webhook to avoid transaction state conflicts
                webhook_session = Session()
                try:
                    # 3. Apply Filters
                    if not _apply_filters(webhook.filters, payload):
                        webhook_session.close()
                        continue

                    # 4. Shape Data
                    shaped_data = _shape_payload(payload, webhook.selected_columns)

                    # 5. Construct Standard Envelope
                    event_payload = {
                        "event": event.event_name,
                        "event_key": event.event_key,
                        "occurred_at": datetime.utcnow().isoformat() + "Z",
                        "source": "action_service",
                        "data": shaped_data,
                    }

                    # Ensure clean JSON
                    event_payload = json.loads(json.dumps(event_payload, default=str))
                    payload_bytes = json.dumps(event_payload).encode("utf-8")

                    # 6. Create Delivery Log
                    delivery = LogWebhookDelivery(
                        webhook_id=webhook.webhook_id,
                        event_id=event.event_id,
                        tenant_id=tenant_id,
                        payload=event_payload,
                        delivery_status="PENDING",
                        creation_date=datetime.utcnow(),
                        attempt_number=1,
                    )
                    webhook_session.add(delivery)
                    webhook_session.flush()

                    # 7. Dispatch
                    _dispatch(webhook, delivery, payload_bytes)

                    # 8. Maintenance
                    if delivery.delivery_status == "FAILED":
                        webhook.failure_count = (webhook.failure_count or 0) + 1
                        if webhook.failure_count >= (webhook.max_retries or 5):
                            webhook.is_active = "N"
                        else:
                            from datetime import timedelta
                            delay = RETRY_DELAYS.get(webhook.failure_count, 3600)
                            delivery.next_retry_date = datetime.utcnow() + timedelta(seconds=delay)
                    else:
                        webhook.failure_count = 0

                    webhook_session.commit()
                except Exception as e:
                    logger.error(
                        f"[Webhook] Webhook dispatch error: {e}", exc_info=True
                    )
                    webhook_session.rollback()
                finally:
                    webhook_session.close()

    except Exception as e:
        logger.error(f"[Webhook] fire crash: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()


def retry_failed_deliveries() -> None:
    """
    Celery Beat task: Retries all FAILED webhook deliveries
    whose next_retry_date is due.
    """
    Session = sessionmaker(bind=db.engine)
    local_session = Session()

    try:
        now = datetime.utcnow()
        pending = (
            local_session.query(LogWebhookDelivery)
            .filter(
                LogWebhookDelivery.delivery_status == "FAILED",
                LogWebhookDelivery.next_retry_date <= now,
            )
            .all()
        )

        for old_delivery in pending:
            retry_session = Session()
            try:
                webhook = retry_session.query(DefWebhook).get(old_delivery.webhook_id)
                if not webhook or webhook.is_active == "N":
                    old_delivery.delivery_status = "PERMANENTLY_FAILED"
                    retry_session.commit()
                    retry_session.close()
                    continue

                new_delivery = LogWebhookDelivery(
                    webhook_id=old_delivery.webhook_id,
                    event_id=old_delivery.event_id,
                    tenant_id=old_delivery.tenant_id,
                    payload=old_delivery.payload,
                    attempt_number=(old_delivery.attempt_number or 1) + 1,
                    delivery_status="PENDING",
                    creation_date=datetime.utcnow(),
                )
                retry_session.add(new_delivery)
                retry_session.flush()

                payload_bytes = json.dumps(old_delivery.payload, default=str).encode("utf-8")
                _dispatch(webhook, new_delivery, payload_bytes)

                if new_delivery.delivery_status == "FAILED":
                    webhook.failure_count = (webhook.failure_count or 0) + 1
                    if webhook.failure_count >= (webhook.max_retries or 5):
                        webhook.is_active = "N"
                    else:
                        from datetime import timedelta
                        delay = RETRY_DELAYS.get(new_delivery.attempt_number, 3600)
                        new_delivery.next_retry_date = datetime.utcnow() + timedelta(seconds=delay)
                else:
                    webhook.failure_count = 0

                old_delivery.next_retry_date = None
                retry_session.commit()
            except Exception as exc:
                logger.error(f"[Webhook] Per-delivery retry error: {exc}", exc_info=True)
                retry_session.rollback()
            finally:
                retry_session.close()
    except Exception as exc:
        local_session.rollback()
        logger.error(f"[Webhook] retry_failed_deliveries crash: {exc}", exc_info=True)
    finally:
        local_session.close()
