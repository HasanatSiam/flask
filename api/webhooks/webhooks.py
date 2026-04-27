# api/webhooks/webhooks.py
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from executors.extensions import db
from executors.models import DefWebhook, DefWebhookEvent, DefWebhookSubscription, DefUser
from . import webhooks_bp

@webhooks_bp.route('/def_webhooks', methods=['GET'])
@jwt_required()
# @role_required()
def get_def_webhooks():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        webhook_id = request.args.get('webhook_id', type=int)
        webhook_name  = request.args.get('webhook_name',  type=str)
        is_active     = request.args.get('is_active',     type=str)
        page          = request.args.get('page',          type=int)
        limit         = request.args.get('limit',         type=int)

        if webhook_id:
            webhook = DefWebhook.query.filter_by(webhook_id=webhook_id).first()
            if webhook:
                # Optional: if tenant_id provided in URL, verify it matches
                if tenant_id and webhook.tenant_id != tenant_id:
                    return make_response(jsonify({'message': 'Webhook found but tenant_id mismatch'}), 403)
                return make_response(jsonify({'result': webhook.json()}), 200)
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        query = DefWebhook.query

        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        elif user_tenant_id != 1:
            # Fallback (lock out if no ID found for non-admin)
            query = query.filter(DefWebhook.tenant_id == -1)
        if webhook_name:
            query = query.filter(DefWebhook.webhook_name.ilike(f'%{webhook_name}%'))
        if is_active:
            query = query.filter_by(is_active=is_active.upper())

        if page and limit:
            paginated = query.order_by(DefWebhook.webhook_id.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            return make_response(jsonify({
                'result': [w.json() for w in paginated.items],
                'total':  paginated.total,
                'pages':  paginated.pages,
                'page':   paginated.page
            }), 200)

        webhooks = query.order_by(DefWebhook.webhook_id.desc()).all()
        return make_response(jsonify({'result': [w.json() for w in webhooks]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching webhooks', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhooks', methods=['POST'])
@jwt_required()
# @role_required()
def create_webhook():
    try:
        data = request.get_json()
        
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_name = data.get('webhook_name')
        webhook_url  = data.get('webhook_url')

        if not webhook_name or not webhook_url or not tenant_id:
            return make_response(jsonify({'message': 'webhook_name and webhook_url are required'}), 400)

        new_webhook = DefWebhook(
            tenant_id=tenant_id,
            webhook_name=webhook_name,
            webhook_url=webhook_url,
            secret_key=data.get('secret_key'),
            extra_headers=data.get('extra_headers'),
            filters=data.get('filters'),
            selected_columns=data.get('selected_columns'),
            is_active=data.get('is_active', 'Y').upper(),
            max_retries=data.get('max_retries', 3),
            created_by=current_user_id,
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow(),
            last_updated_by=current_user_id
        )
        db.session.add(new_webhook)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully', 'result': new_webhook.json()}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error adding webhook', 'error': str(e)}), 500)


@webhooks_bp.route('/def_webhooks/with-subscriptions', methods=['POST'])
@jwt_required()
# @role_required()
def create_webhook_with_subscriptions():
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

        event_ids = data.get('event_ids')
        if event_ids is None:
            return make_response(jsonify({'message': 'event_ids is required'}), 400)
        if not isinstance(event_ids, list) or not event_ids:
            return make_response(jsonify({'message': 'event_ids must be a non-empty list'}), 400)

        normalized_event_ids = []
        for event_id in event_ids:
            if not isinstance(event_id, int):
                return make_response(jsonify({'message': 'All event_ids must be integers'}), 400)
            if event_id not in normalized_event_ids:
                normalized_event_ids.append(event_id)

        valid_events = DefWebhookEvent.query.filter(
            DefWebhookEvent.event_id.in_(normalized_event_ids)
        ).all()
        valid_event_ids = [event.event_id for event in valid_events]
        invalid_event_ids = [
            event_id for event_id in normalized_event_ids if event_id not in valid_event_ids
        ]
        if not valid_event_ids:
            return make_response(jsonify({'message': 'event_ids are invalid'}), 400)

        new_webhook = DefWebhook(
            tenant_id=tenant_id,
            webhook_name=webhook_name,
            webhook_url=webhook_url,
            secret_key=data.get('secret_key'),
            extra_headers=data.get('extra_headers'),
            filters=data.get('filters'),
            selected_columns=data.get('selected_columns'),
            is_active=data.get('is_active', 'Y').upper(),
            max_retries=data.get('max_retries', 3),
            created_by=current_user_id,
            creation_date=datetime.utcnow()
        )
        db.session.add(new_webhook)
        db.session.flush()


        created_subs = []
        existing_event_ids = []
        for event_id in valid_event_ids:
            new_sub = DefWebhookSubscription(
                tenant_id=tenant_id,
                webhook_id=new_webhook.webhook_id,
                event_id=event_id,
                created_by=current_user_id,
                creation_date=datetime.utcnow(),
                last_updated_by=current_user_id,
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_sub)
            created_subs.append(new_sub)

        db.session.commit()

        return make_response(jsonify({
            'message': 'Added successfully',
            'created_count': len(created_subs),
            'skipped_existing_count': len(existing_event_ids),
            'invalid_event_ids': invalid_event_ids,
            'result': [s.json() for s in created_subs]
        }), 201 if created_subs else 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error adding webhook with subscriptions',
            'error': str(e)
        }), 500)

@webhooks_bp.route('/def_webhooks', methods=['PUT'])
@jwt_required()
# @role_required()
def update_webhook():
    try:
        webhook_id = request.args.get('webhook_id', type=int)
        if not webhook_id:
            return make_response(jsonify({'message': 'webhook_id is required'}), 400)

        webhook = DefWebhook.query.filter_by(webhook_id=webhook_id).first()
        if not webhook:
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        data = request.get_json()
        webhook.webhook_name     = data.get('webhook_name',     webhook.webhook_name)
        webhook.webhook_url      = data.get('webhook_url',      webhook.webhook_url)
        webhook.secret_key       = data.get('secret_key',       webhook.secret_key)
        webhook.extra_headers    = data.get('extra_headers',    webhook.extra_headers)
        webhook.filters          = data.get('filters',          webhook.filters)
        webhook.selected_columns = data.get('selected_columns', webhook.selected_columns)
        webhook.is_active        = data.get('is_active',        webhook.is_active)
        webhook.max_retries      = data.get('max_retries',      webhook.max_retries)
        webhook.last_updated_by  = get_jwt_identity()
        webhook.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error editing webhook', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhooks', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_webhook():
    try:
        data = request.get_json()
        if not data or 'webhook_ids' not in data:
            return make_response(jsonify({'message': 'webhook_ids (list) is required'}), 400)

        ids = data.get('webhook_ids')
        webhooks = DefWebhook.query.filter(DefWebhook.webhook_id.in_(ids)).all()
        
        if not webhooks:
            return make_response(jsonify({'message': 'No webhooks found for provided IDs'}), 404)

        for w in webhooks:
            db.session.delete(w)

        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting webhooks', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhooks/toggle', methods=['PATCH'])
@jwt_required()
# @role_required()
def toggle_webhook():
    try:
        webhook_id = request.args.get('webhook_id', type=int)
        webhook = DefWebhook.query.get(webhook_id)
        if not webhook:
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        webhook.is_active        = 'N' if webhook.is_active == 'Y' else 'Y'
        if webhook.is_active == 'Y':
            webhook.failure_count = 0  # reset failure count on manual reactivation
            
        webhook.last_updated_by  = get_jwt_identity()
        webhook.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({
            'message': f"Webhook {'activated' if webhook.is_active == 'Y' else 'deactivated'} successfully",
            'is_active': webhook.is_active
        }), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error toggling webhook', 'error': str(e)}), 500)
