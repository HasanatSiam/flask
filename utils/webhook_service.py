# webhook_service.py
"""
Automatic Outbound Webhook Service
==================================
This service listens for database commits and automatically dispatches webhooks
based on table activity (Insert, Update, Delete).

How it works
------------
1. The `handle_models_committed` signal listener captures successful DB transactions.
2. It identifies the table, the operation (POST/PUT/DELETE), and the tenant.
3. It queries DEF_WEBHOOKS for active hooks matching the table + method + tenant.
4. It shapes the data (obeying selected_columns) and wraps it in a standard event.
5. It handles delivery, retries, and failure logging.
"""

import hashlib
import hmac
import json
import logging
import time

from datetime import datetime, timedelta

import requests
from flask import g
from flask_sqlalchemy.track_modifications import models_committed
from flask_jwt_extended import get_jwt_identity
from flask import has_request_context
from executors.models import DefUser
from sqlalchemy.orm import sessionmaker

from executors.extensions import db
from executors.models import DefWebhook, LogWebhookDelivery

logger = logging.getLogger(__name__)

# ── Backoff schedule (seconds) per attempt number ────────────────────────────
RETRY_DELAYS = {
    1: 60,        # retry 1 → 1 min
    2: 300,       # retry 2 → 5 min
    3: 900,       # retry 3 → 15 min
    4: 1800,      # retry 4 → 30 min
    5: 3600,      # retry 5 → 1 hr
}


# ── Internal helpers ──────────────────────────────────────────────────────────

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


def _shape_data(data: dict, selected_columns: list | None) -> dict:
    """Return a subset of the data based on selected_columns."""
    if not selected_columns or not isinstance(selected_columns, list):
        return data
    return {k: v for k, v in data.items() if k in selected_columns}


def _dispatch(webhook: DefWebhook, delivery: LogWebhookDelivery, payload_bytes: bytes) -> None:
    """Perform the actual HTTP call."""
    headers = {"Content-Type": "application/json"}
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

        delivery.http_status_code = response.status_code
        delivery.response_body    = response.text[:4000]
        delivery.duration_ms      = duration_ms

        if 200 <= response.status_code < 300:
            delivery.delivery_status = "DELIVERED"
        else:
            delivery.delivery_status = "FAILED"
            delivery.error_message   = f"HTTP {response.status_code}"

    except Exception as exc:
        delivery.delivery_status = "FAILED"
        delivery.error_message   = str(exc)[:500]
        delivery.duration_ms     = int((time.time() - start) * 1000)


# ── Database Listener ─────────────────────────────────────────────────────────

def init_webhook_listener(app):
    """Initialize the global database signal listener."""
    models_committed.connect(handle_models_committed, sender=app)
    logger.info("[Webhook] Automatic DB listener initialized.")


def handle_models_committed(sender, changes):
    """Signal handler triggered after successful DB commit."""
    # Get the actor's tenant ID if possible
    actor_tenant_id = None
    if has_request_context():
        # First check 'g' (cached)
        if hasattr(g, 'actor_tenant_id'):
            actor_tenant_id = g.actor_tenant_id
        else:
            # Fallback: check JWT claims if we added it there, or query DB
            # For now, let's query DB via the actor's user_id if we have it
            user_id = get_jwt_identity()
            if user_id:
                Session = sessionmaker(bind=db.engine)
                temp_session = Session()
                try:
                    user = temp_session.query(DefUser).get(int(user_id))
                    if user:
                        actor_tenant_id = user.tenant_id
                        g.actor_tenant_id = actor_tenant_id # Cache for this request
                finally:
                    temp_session.close()

    for instance, operation in changes:
        # Avoid recursive loops
        if isinstance(instance, (DefWebhook, LogWebhookDelivery)):
            continue

        table_name = getattr(instance, '__tablename__', None)
        if not table_name:
            continue

        method_map = {'insert': 'POST', 'update': 'PUT', 'delete': 'DELETE'}
        method = method_map.get(operation)
        if not method:
            continue

        # Collect potential interested tenants
        interested_tenants = set()
        
        # 1. The tenant the record belongs to
        record_tenant_id = getattr(instance, 'tenant_id', None)
        if record_tenant_id:
            interested_tenants.add(record_tenant_id)
            
        # 2. The tenant who performed the action (The Actor)
        if actor_tenant_id:
            interested_tenants.add(actor_tenant_id)

        if not interested_tenants:
            continue

        # Get the full record data
        full_data = instance.json() if hasattr(instance, 'json') else {}
        if not full_data:
            continue

        # Fire the webhook for each interested tenant
        # (This handles the case where admin wants to see sub-tenant creation)
        fire(
            table_name=table_name,
            method=method,
            tenant_ids=list(interested_tenants),
            payload=full_data
        )


# ── Core Logic ────────────────────────────────────────────────────────────────

def fire(table_name: str, method: str, tenant_ids: list[int], payload: dict) -> list[int]:
    """
    Find matching webhooks and dispatch them.
    Can accept a single tenant_id (int) or a list of tenant_ids.
    """
    if isinstance(tenant_ids, int):
        tenant_ids = [tenant_ids]

    Session = sessionmaker(bind=db.engine)
    local_session = Session()

    delivery_ids = []
    try:
        # Find matching webhooks: matching table, any of the tenant IDs, and method exists in array
        webhooks = local_session.query(DefWebhook).filter(
            DefWebhook.tenant_id.in_(tenant_ids),
            DefWebhook.table_name == table_name,
            DefWebhook.http_methods.contains([method]),
            DefWebhook.is_active == 'Y'
        ).all()

        if not webhooks:
            return delivery_ids

        for webhook in webhooks:
            # 1. Apply Filters
            if not _apply_filters(webhook.filters, payload):
                continue

            # 2. Shape Payload (Column Selection)
            shaped_data = _shape_data(payload, webhook.selected_columns)

            # 3. Create Standard Event Wrapper
            event_payload = {
                "event": f"{table_name}.{method.lower()}",
                "table": table_name,
                "occurred_at": datetime.utcnow().isoformat() + "Z",
                "webhook_name": webhook.webhook_name,
                "data": shaped_data
            }
            # Ensure the structure is JSON-serializable (converts datetimes to strings etc.)
            event_payload = json.loads(json.dumps(event_payload, default=str)) 
            payload_bytes = json.dumps(event_payload).encode("utf-8")

            # 4. Create Delivery Log
            delivery = LogWebhookDelivery(
                webhook_id      = webhook.webhook_id,
                tenant_id       = webhook.tenant_id,  # Use the webhook's owner tenant ID
                event_name      = event_payload["event"],
                table_name      = table_name,
                trigger_method  = method,
                payload         = event_payload,
                attempt_number  = 1,
                delivery_status = "PENDING",
                creation_date   = datetime.utcnow(),
            )
            local_session.add(delivery)
            local_session.flush()

            # 5. Dispatch HTTP
            _dispatch(webhook, delivery, payload_bytes)

            # 6. Maintenance
            if delivery.delivery_status == "FAILED":
                webhook.failure_count = (webhook.failure_count or 0) + 1
                if webhook.failure_count >= (webhook.max_retries or 5):
                    webhook.is_active = 'N'
                else:
                    delay = RETRY_DELAYS.get(webhook.failure_count, 3600)
                    delivery.next_retry_date = datetime.utcnow() + timedelta(seconds=delay)
            else:
                webhook.failure_count = 0

            local_session.commit()
            delivery_ids.append(delivery.delivery_id)

    except Exception as exc:
        local_session.rollback()
        logger.error(f"[Webhook] fire() automatic crash: {exc}", exc_info=True)
    finally:
        local_session.close()

    return delivery_ids


def retry_failed_deliveries() -> None:
    """Retry logic for Celery Beat."""
    Session = sessionmaker(bind=db.engine)
    local_session = Session()

    try:
        now = datetime.utcnow()
        pending = local_session.query(LogWebhookDelivery).filter(
            LogWebhookDelivery.delivery_status == "FAILED",
            LogWebhookDelivery.next_retry_date <= now
        ).all()

        for old_delivery in pending:
            webhook = local_session.query(DefWebhook).get(old_delivery.webhook_id)
            if not webhook or webhook.is_active == 'N':
                old_delivery.delivery_status = "PERMANENTLY_FAILED"
                local_session.commit()
                continue

            new_delivery = LogWebhookDelivery(
                webhook_id      = old_delivery.webhook_id,
                tenant_id       = old_delivery.tenant_id,
                event_name      = old_delivery.event_name,
                table_name      = old_delivery.table_name,
                payload         = old_delivery.payload,
                attempt_number  = (old_delivery.attempt_number or 1) + 1,
                delivery_status = "PENDING",
                creation_date   = datetime.utcnow(),
            )
            local_session.add(new_delivery)
            local_session.flush()

            payload_bytes = json.dumps(old_delivery.payload, default=str).encode("utf-8")
            _dispatch(webhook, new_delivery, payload_bytes)

            if new_delivery.delivery_status == "FAILED":
                webhook.failure_count = (webhook.failure_count or 0) + 1
                if webhook.failure_count >= (webhook.max_retries or 5):
                    webhook.is_active = 'N'
                else:
                    delay = RETRY_DELAYS.get(new_delivery.attempt_number, 3600)
                    new_delivery.next_retry_date = datetime.utcnow() + timedelta(seconds=delay)
            else:
                webhook.failure_count = 0

            old_delivery.next_retry_date = None
            local_session.commit()
    except Exception as exc:
        local_session.rollback()
        logger.error(f"[Webhook] retry_failed_deliveries crash: {exc}", exc_info=True)
    finally:
        local_session.close()
