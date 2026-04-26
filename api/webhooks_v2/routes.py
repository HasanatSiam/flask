# api/webhooks_v2/routes.py
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefWebhookV2, 
    DefWebhookEventV2, 
    DefWebhookSubscriptionV2, 
    LogWebhookDeliveryV2,
    DefApiEndpoint,
    DefPrivilege,
    DefUser
)

from . import webhooks_v2_bp

# ==============================================================================
# --- WEBHOOK V2 REGISTRY ---
# ==============================================================================

@webhooks_v2_bp.route('/def_webhooks_v2', methods=['GET'])
@jwt_required()
# @role_required()
def get_def_webhooks_v2():
    try:
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_v2_id = request.args.get('webhook_v2_id', type=int)
        webhook_name  = request.args.get('webhook_name',  type=str)
        is_active     = request.args.get('is_active',     type=str)
        page          = request.args.get('page',          type=int)
        limit         = request.args.get('limit',         type=int)

        if webhook_v2_id:
            webhook = DefWebhookV2.query.filter_by(webhook_v2_id=webhook_v2_id).first()
            if webhook:
                return make_response(jsonify({'result': webhook.json()}), 200)
            return make_response(jsonify({'message': 'Webhook V2 not found'}), 404)

        query = DefWebhookV2.query

        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_name:
            query = query.filter(DefWebhookV2.webhook_name.ilike(f'%{webhook_name}%'))
        if is_active:
            query = query.filter_by(is_active=is_active.upper())

        if page and limit:
            paginated = query.order_by(DefWebhookV2.webhook_v2_id.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            return make_response(jsonify({
                'result': [w.json() for w in paginated.items],
                'total':  paginated.total,
                'pages':  paginated.pages,
                'page':   paginated.page
            }), 200)

        webhooks = query.order_by(DefWebhookV2.webhook_v2_id.desc()).all()
        return make_response(jsonify({'result': [w.json() for w in webhooks]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching webhooks V2', 'error': str(e)}), 500)

@webhooks_v2_bp.route('/def_webhooks_v2', methods=['POST'])
@jwt_required()
# @role_required()
def create_webhook_v2():
    try:
        data = request.get_json()
        
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_name = data.get('webhook_name')
        webhook_url  = data.get('webhook_url')

        if not webhook_name or not webhook_url or not tenant_id:
            return make_response(jsonify({'message': 'webhook_name and webhook_url are required'}), 400)

        new_webhook = DefWebhookV2(
            tenant_id=tenant_id,
            webhook_name=webhook_name,
            webhook_url=webhook_url,
            secret_key=data.get('secret_key'),
            extra_headers=data.get('extra_headers'),
            filters=data.get('filters'),
            selected_columns=data.get('selected_columns'),
            is_active=data.get('is_active', 'Y').upper(),
            created_by=current_user_id,
            creation_date=datetime.utcnow()
        )
        db.session.add(new_webhook)
        db.session.commit()
        return make_response(jsonify({'message': 'V2 Webhook added successfully', 'result': new_webhook.json()}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error adding V2 webhook', 'error': str(e)}), 500)


@webhooks_v2_bp.route('/def_webhooks_v2/with-subscriptions', methods=['POST'])
@jwt_required()
# @role_required()
def create_webhook_v2_with_subscriptions():
    try:
        data = request.get_json() or {}

        current_user_id = get_jwt_identity()
        user = DefUser.query.get(int(current_user_id))
        tenant_id = user.tenant_id if user else None
        webhook_name = data.get('webhook_name')
        webhook_url = data.get('webhook_url')

        if not webhook_name or not webhook_url:
            return make_response(jsonify({'message': 'webhook_name and webhook_url are required'}), 400)
        if not tenant_id:
            return make_response(jsonify({'message': 'Unable to resolve tenant for current user'}), 400)

        event_v2_ids = data.get('event_v2_ids')
        if event_v2_ids is None:
            return make_response(jsonify({'message': 'event_v2_ids is required'}), 400)
        if not isinstance(event_v2_ids, list) or not event_v2_ids:
            return make_response(jsonify({'message': 'event_v2_ids must be a non-empty list'}), 400)

        normalized_event_ids = []
        for event_id in event_v2_ids:
            if not isinstance(event_id, int):
                return make_response(jsonify({'message': 'All event_v2_ids must be integers'}), 400)
            if event_id not in normalized_event_ids:
                normalized_event_ids.append(event_id)

        valid_events = DefWebhookEventV2.query.filter(
            DefWebhookEventV2.event_v2_id.in_(normalized_event_ids)
        ).all()
        valid_event_ids = [event.event_v2_id for event in valid_events]
        invalid_event_ids = [
            event_id for event_id in normalized_event_ids if event_id not in valid_event_ids
        ]
        if not valid_event_ids:
            return make_response(jsonify({'message': 'event_v2_ids are invalid'}), 400)

        new_webhook = DefWebhookV2(
            tenant_id=tenant_id,
            webhook_name=webhook_name,
            webhook_url=webhook_url,
            secret_key=data.get('secret_key'),
            extra_headers=data.get('extra_headers'),
            filters=data.get('filters'),
            selected_columns=data.get('selected_columns'),
            is_active=data.get('is_active', 'Y').upper(),
            created_by=current_user_id,
            creation_date=datetime.utcnow()
        )
        db.session.add(new_webhook)
        db.session.flush()


        created_subs = []
        for event_id in valid_event_ids:
            new_sub = DefWebhookSubscriptionV2(
                tenant_id=tenant_id,
                webhook_v2_id=new_webhook.webhook_v2_id,
                event_v2_id=event_id,
                created_by=current_user_id,
                creation_date=datetime.utcnow(),
                last_updated_by=current_user_id,
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_sub)
            created_subs.append(new_sub)

        db.session.commit()

        return make_response(jsonify({
            'message': 'V2 Webhook and subscriptions created successfully',
            'result': {
                'webhook': new_webhook.json(),
                'subscriptions': [sub.json() for sub in created_subs],
                'created_count': len(created_subs),
                'invalid_event_ids': invalid_event_ids
            }
        }), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error adding V2 webhook with subscriptions',
            'error': str(e)
        }), 500)

@webhooks_v2_bp.route('/def_webhooks_v2', methods=['PUT'])
@jwt_required()
# @role_required()
def update_webhook_v2():
    try:
        webhook_v2_id = request.args.get('webhook_v2_id', type=int)
        if not webhook_v2_id:
            return make_response(jsonify({'message': 'webhook_v2_id is required'}), 400)

        webhook = DefWebhookV2.query.filter_by(webhook_v2_id=webhook_v2_id).first()
        if not webhook:
            return make_response(jsonify({'message': 'V2 Webhook not found'}), 404)

        data = request.get_json()
        webhook.webhook_name     = data.get('webhook_name',     webhook.webhook_name)
        webhook.webhook_url      = data.get('webhook_url',      webhook.webhook_url)
        webhook.secret_key       = data.get('secret_key',       webhook.secret_key)
        webhook.extra_headers    = data.get('extra_headers',    webhook.extra_headers)
        webhook.filters          = data.get('filters',          webhook.filters)
        webhook.selected_columns = data.get('selected_columns', webhook.selected_columns)
        webhook.is_active        = data.get('is_active',        webhook.is_active)
        webhook.last_updated_by  = get_jwt_identity()
        webhook.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'V2 Webhook edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error editing V2 webhook', 'error': str(e)}), 500)

@webhooks_v2_bp.route('/def_webhooks_v2', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_webhook_v2():
    try:
        data = request.get_json()
        if not data or 'webhook_v2_ids' not in data:
            return make_response(jsonify({'message': 'webhook_v2_ids (list) is required'}), 400)

        ids = data.get('webhook_v2_ids')
        webhooks = DefWebhookV2.query.filter(DefWebhookV2.webhook_v2_id.in_(ids)).all()
        
        if not webhooks:
            return make_response(jsonify({'message': 'No V2 webhooks found for provided IDs'}), 404)

        for w in webhooks:
            db.session.delete(w)

        db.session.commit()
        return make_response(jsonify({'message': f'{len(webhooks)} V2 webhook(s) deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting V2 webhooks', 'error': str(e)}), 500)

@webhooks_v2_bp.route('/def_webhooks_v2/toggle', methods=['PATCH'])
@jwt_required()
# @role_required()
def toggle_webhook_v2():
    try:
        webhook_v2_id = request.args.get('webhook_v2_id', type=int)
        webhook = DefWebhookV2.query.get(webhook_v2_id)
        if not webhook:
            return make_response(jsonify({'message': 'V2 Webhook not found'}), 404)

        webhook.is_active        = 'N' if webhook.is_active == 'Y' else 'Y'
        webhook.last_updated_by  = get_jwt_identity()
        webhook.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({
            'message': f"V2 Webhook {'activated' if webhook.is_active == 'Y' else 'deactivated'} successfully",
            'is_active': webhook.is_active
        }), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error toggling V2 webhook', 'error': str(e)}), 500)


# --- DELIVERY LOGS ---


@webhooks_v2_bp.route('/log_webhook_deliveries_v2', methods=['GET'])
@jwt_required()
# @role_required()
def get_webhook_deliveries_v2():
    try:
        tenant_id    = request.args.get('tenant_id',    type=int)
        webhook_name = request.args.get('webhook_name', type=str)
        event_name   = request.args.get('event_name',   type=str)
        page         = request.args.get('page',         type=int, default=1)
        limit        = request.args.get('limit',        type=int, default=20)

        query = db.session.query(
            LogWebhookDeliveryV2,
            DefWebhookV2.webhook_name,
            DefWebhookEventV2.event_name
        ).outerjoin(
            DefWebhookV2,
            LogWebhookDeliveryV2.webhook_v2_id == DefWebhookV2.webhook_v2_id
        ).outerjoin(
            DefWebhookEventV2,
            LogWebhookDeliveryV2.event_v2_id == DefWebhookEventV2.event_v2_id
        )

        if tenant_id:
            query = query.filter(LogWebhookDeliveryV2.tenant_id == tenant_id)
        if webhook_name:
            query = query.filter(DefWebhookV2.webhook_name.ilike(f'%{webhook_name}%'))
        if event_name:
            query = query.filter(DefWebhookEventV2.event_name.ilike(f'%{event_name}%'))

        paginated = query.order_by(
            LogWebhookDeliveryV2.delivery_v2_id.desc()
        ).paginate(page=page, per_page=limit, error_out=False)

        result = []
        for delivery, joined_webhook_name, joined_event_name in paginated.items:
            item = delivery.json()
            item['webhook_name'] = joined_webhook_name
            item['event_name'] = joined_event_name
            result.append(item)

        return make_response(jsonify({
            'result': result,
            'total': paginated.total,
            'pages': 1 if paginated.total == 0 else paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching V2 delivery logs', 'error': str(e)}), 500)

# ==============================================================================
# --- EVENT DISCOVERY & CATALOG ---
# ==============================================================================

@webhooks_v2_bp.route('/def_webhook_events_v2', methods=['GET'])
@jwt_required()
@role_required()
def get_webhook_events_v2():
    try:
        # Join with DefApiEndpoint and DefPrivilege to provide a "Better Name" experience
        results = db.session.query(
            DefWebhookEventV2,
            DefApiEndpoint.api_endpoint,
            DefApiEndpoint.method,
            DefPrivilege.privilege_name
        ).join(
            DefApiEndpoint, DefWebhookEventV2.api_endpoint_id == DefApiEndpoint.api_endpoint_id
        ).outerjoin(
            DefPrivilege, DefApiEndpoint.privilege_id == DefPrivilege.privilege_id
        ).all()

        catalog = []
        for event, path, method, priv in results:
            item = event.json()
            item['technical_details'] = {
                'path': path,
                'method': method,
                'privilege': priv or "Unassigned"
            }
            # The "Better Name" for the UI is derived from the friendly event_name
            catalog.append(item)

        return make_response(jsonify({'result': catalog}), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching event catalog', 'error': str(e)}), 500)

@webhooks_v2_bp.route('/def_webhook_events_v2', methods=['POST'])
@jwt_required()
# @role_required()
def register_business_event():
    """Manual registration of a business event linked to a technical endpoint."""
    try:
        data = request.get_json()
        new_event = DefWebhookEventV2(
            api_endpoint_id = data.get('api_endpoint_id'),
            event_name      = data.get('event_name'),
            event_key       = data.get('event_key'),
            description     = data.get('description'),
            created_by      = get_jwt_identity(),
            creation_date   = datetime.utcnow(),
            last_update_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
        )
        db.session.add(new_event)
        db.session.commit()
        return make_response(jsonify({'message': 'Business event registered', 'result': new_event.json()}), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error registering event', 'error': str(e)}), 500)


# --- SUBSCRIPTIONS ---


@webhooks_v2_bp.route('/def_webhook_subscriptions_v2', methods=['POST'])
@jwt_required()
# @role_required()
def subscribe_webhook_v2():
    try:
        data = request.get_json()
        
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_v2_id = data.get('webhook_v2_id')
        if not tenant_id or not webhook_v2_id:
            return make_response(jsonify({'message': 'tenant and webhook_v2_id are required'}), 400)

        # Backward compatible:
        # - old payload: {"webhook_v2_id": 1, "event_v2_id": 10}
        # - new payload: {"webhook_v2_id": 1, "event_v2_ids": [10, 11]}
        event_v2_ids = data.get('event_v2_ids')
        if event_v2_ids is None:
            single_event_id = data.get('event_v2_id')
            event_v2_ids = [single_event_id] if single_event_id is not None else []

        if event_v2_ids is None:
            return make_response(jsonify({'message': 'event_v2_ids is required'}), 400)
        if not isinstance(event_v2_ids, list) or not event_v2_ids:
            return make_response(jsonify({'message': 'event_v2_ids must be a non-empty list'}), 400)

        normalized_event_ids = []
        for event_id in event_v2_ids:
            if not isinstance(event_id, int):
                return make_response(jsonify({'message': 'All event_v2_ids must be integers'}), 400)
            if event_id not in normalized_event_ids:
                normalized_event_ids.append(event_id)

        # Ensure webhook exists for this tenant.
        webhook = DefWebhookV2.query.filter_by(
            webhook_v2_id=webhook_v2_id,
            tenant_id=tenant_id
        ).first()
        if not webhook:
            return make_response(jsonify({'message': 'V2 Webhook not found'}), 404)

        # Keep only existing events.
        valid_events = DefWebhookEventV2.query.filter(
            DefWebhookEventV2.event_v2_id.in_(normalized_event_ids)
        ).all()
        valid_event_ids = [event.event_v2_id for event in valid_events]
        invalid_event_ids = [
            event_id for event_id in normalized_event_ids if event_id not in valid_event_ids
        ]

        if not valid_event_ids:
            return make_response(jsonify({'message': 'event_v2_ids are invalid'}), 400)

        # Avoid duplicates (idempotent behavior).
        existing_subs = DefWebhookSubscriptionV2.query.filter(
            DefWebhookSubscriptionV2.tenant_id == tenant_id,
            DefWebhookSubscriptionV2.webhook_v2_id == webhook_v2_id,
            DefWebhookSubscriptionV2.event_v2_id.in_(valid_event_ids)
        ).all()
        existing_event_ids = [s.event_v2_id for s in existing_subs]

        created_subs = []
        now = datetime.utcnow()
        for event_id in valid_event_ids:
            if event_id in existing_event_ids:
                continue
            new_sub = DefWebhookSubscriptionV2(
                tenant_id=tenant_id,
                webhook_v2_id=webhook_v2_id,
                event_v2_id=event_id,
                created_by=current_user_id,
                creation_date=now,
                last_updated_by=current_user_id,
                last_update_date=now
            )
            db.session.add(new_sub)
            created_subs.append(new_sub)

        db.session.commit()

        return make_response(jsonify({
            'message': 'Added successfully' if created_subs else 'No new subscriptions added',
            'created_count': len(created_subs),
            'skipped_existing_count': len(existing_event_ids),
            'invalid_event_ids': invalid_event_ids,
            'result': [s.json() for s in created_subs]
        }), 201 if created_subs else 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating subscription', 'error': str(e)}), 500)

@webhooks_v2_bp.route('/def_webhook_subscriptions_v2', methods=['GET'])
@jwt_required()
# @role_required()
def get_subscriptions_v2():
    try:
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_v2_id = request.args.get('webhook_v2_id', type=int)
        
        query = DefWebhookSubscriptionV2.query
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_v2_id:
            query = query.filter_by(webhook_v2_id=webhook_v2_id)
            
        subs = query.all()
        return make_response(jsonify({'result': [s.json() for s in subs]}), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching subscriptions', 'error': str(e)}), 500)
