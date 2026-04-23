# utils/webhook_service_v2.py
"""
Service B: Endpoint-Centric Webhook Service (Testing)
=====================================================
This service dispatches webhooks based on specific API actions (Endpoint IDs)
rather than database table commits. It provides a standardized envelope.
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
    DefWebhookV2,
    DefWebhookSubscriptionV2,
    LogWebhookDeliveryV2,
    DefWebhookEventV2,
)

logger = logging.getLogger(__name__)


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
    webhook: DefWebhookV2, delivery: LogWebhookDeliveryV2, payload_bytes: bytes
) -> None:
    """Perform the actual HTTP call and update delivery log."""
    headers = {"Content-Type": "application/json"}

    # Add custom extra headers
    if webhook.extra_headers:
        headers.update(webhook.extra_headers)

    if webhook.secret_key:
        signature = _sign_payload(webhook.secret_key, payload_bytes)
        headers["X-PROCG-Signature-V2"] = f"sha256={signature}"

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


def fire_v2(api_endpoint_id: int, payload: dict, tenant_id: int) -> None:
    """
    Finds all business events and active webhooks for a given endpoint and dispatches them.
    Supports filters and data shaping (selected columns).
    """
    Session = sessionmaker(bind=db.engine)
    session = Session()

    try:
        # 1. Find all Business Events associated with this technical endpoint
        events = (
            session.query(DefWebhookEventV2)
            .filter_by(api_endpoint_id=api_endpoint_id)
            .all()
        )
        if not events:
            return

        for event in events:
            # 2. Find all active webhooks subscribed to this specific business event for this tenant
            subscriptions = (
                session.query(DefWebhookSubscriptionV2, DefWebhookV2)
                .join(
                    DefWebhookV2,
                    DefWebhookSubscriptionV2.webhook_v2_id
                    == DefWebhookV2.webhook_v2_id,
                )
                .filter(
                    DefWebhookSubscriptionV2.event_v2_id == event.event_v2_id,
                    DefWebhookV2.tenant_id == tenant_id,
                    DefWebhookV2.is_active == "Y",
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

                    # 5. Construct Standard V2 Envelope
                    event_payload = {
                        "event": event.event_name,
                        "event_key": event.event_key,
                        "occurred_at": datetime.utcnow().isoformat() + "Z",
                        "source": "v2_action_service",
                        "data": shaped_data,
                    }

                    # Ensure clean JSON
                    event_payload = json.loads(json.dumps(event_payload, default=str))
                    payload_bytes = json.dumps(event_payload).encode("utf-8")

                    # 6. Create Delivery Log
                    delivery = LogWebhookDeliveryV2(
                        webhook_v2_id=webhook.webhook_v2_id,
                        event_v2_id=event.event_v2_id,
                        tenant_id=tenant_id,
                        payload=event_payload,
                        delivery_status="PENDING",
                        creation_date=datetime.utcnow(),
                    )
                    webhook_session.add(delivery)
                    webhook_session.flush()

                    # 7. Dispatch
                    _dispatch(webhook, delivery, payload_bytes)

                    webhook_session.commit()
                except Exception as e:
                    logger.error(
                        f"[WebhookV2] Webhook dispatch error: {e}", exc_info=True
                    )
                    webhook_session.rollback()
                finally:
                    webhook_session.close()

    except Exception as e:
        logger.error(f"[WebhookV2] fire_v2 crash: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
