# api/webhooks/subscriptions.py
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from executors.extensions import db
from executors.models import DefWebhook, DefWebhookEvent, DefWebhookSubscription, DefUser, DefWebhookSubscriptionV
from . import webhooks_bp

@webhooks_bp.route('/def_webhook_subscriptions', methods=['POST'])
@jwt_required()
# @role_required()
def subscribe_webhook():
    try:
        data = request.get_json()
        
        current_user_id = get_jwt_identity()
        user            = DefUser.query.get(int(current_user_id))
        tenant_id       = user.tenant_id if user else None

        webhook_id = data.get('webhook_id')
        if not tenant_id or not webhook_id:
            return make_response(jsonify({'message': 'tenant and webhook_id are required'}), 400)

        # Backward compatible:
        # - old payload: {"webhook_id": 1, "event_id": 10}
        # - new payload: {"webhook_id": 1, "event_ids": [10, 11]}
        event_ids = data.get('event_ids')
        if event_ids is None:
            single_event_id = data.get('event_id')
            event_ids = [single_event_id] if single_event_id is not None else []

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

        # Ensure webhook exists for this tenant.
        webhook = DefWebhook.query.filter_by(
            webhook_id=webhook_id,
            tenant_id=tenant_id
        ).first()
        if not webhook:
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        # Keep only existing events.
        valid_events = DefWebhookEvent.query.filter(
            DefWebhookEvent.event_id.in_(normalized_event_ids)
        ).all()
        valid_event_ids = [event.event_id for event in valid_events]
        invalid_event_ids = [
            event_id for event_id in normalized_event_ids if event_id not in valid_event_ids
        ]

        if not valid_event_ids:
            return make_response(jsonify({'message': 'event_ids are invalid'}), 400)

        # Avoid duplicates (idempotent behavior).
        existing_subs = DefWebhookSubscription.query.filter(
            DefWebhookSubscription.tenant_id == tenant_id,
            DefWebhookSubscription.webhook_id == webhook_id,
            DefWebhookSubscription.event_id.in_(valid_event_ids)
        ).all()
        existing_event_ids = [s.event_id for s in existing_subs]

        created_subs = []
        now = datetime.utcnow()
        for event_id in valid_event_ids:
            if event_id in existing_event_ids:
                continue
            new_sub = DefWebhookSubscription(
                tenant_id=tenant_id,
                webhook_id=webhook_id,
                event_id=event_id,
                created_by=current_user_id,
                creation_date=now,
                last_updated_by=current_user_id,
                last_update_date=now
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
        return make_response(jsonify({'message': 'Error creating subscription', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_subscriptions', methods=['GET'])
@jwt_required()
# @role_required()
def get_subscriptions():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        webhook_id = request.args.get('webhook_id', type=int)
        
        query = DefWebhookSubscription.query
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_id:
            query = query.filter_by(webhook_id=webhook_id)
            
        subs = query.all()
        return make_response(jsonify({'result': [s.json() for s in subs]}), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching subscriptions', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_subscriptions', methods=['PUT'])
@jwt_required()
# @role_required()
def update_subscription():
    """
    Update subscriptions.
    Case 1: (Single) Provide 'subscription_id' in query to update one record.
    Case 2: (Bulk Sync) Provide 'webhook_id' and 'event_ids' (list) in body to reconcile all events for a webhook.
    """
    try:
        current_user_id = get_jwt_identity()
        user = DefUser.query.get(int(current_user_id))
        tenant_id = user.tenant_id if user else None
        
        data = request.get_json() or {}
        sub_id = request.args.get('subscription_id', type=int)

        # CASE 1: Single Subscription Update
        if sub_id:
            sub = DefWebhookSubscription.query.get(sub_id)
            if not sub:
                return make_response(jsonify({'message': 'Subscription not found'}), 404)
            if tenant_id and sub.tenant_id != tenant_id:
                return make_response(jsonify({'message': 'Access denied'}), 403)

            if 'webhook_id' in data:
                sub.webhook_id = data.get('webhook_id')
            if 'event_id' in data:
                sub.event_id = data.get('event_id')

            sub.last_updated_by = current_user_id
            sub.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully', 'result': sub.json()}), 200)

        # CASE 2: Bulk Sync (Reconcile)
        webhook_id = data.get('webhook_id')
        new_event_ids = data.get('event_ids')

        if webhook_id and isinstance(new_event_ids, list):
            # Verify Webhook Ownership
            webhook = DefWebhook.query.filter_by(webhook_id=webhook_id, tenant_id=tenant_id).first()
            if not webhook:
                return make_response(jsonify({'message': 'Webhook not found'}), 404)

            # Reconcile logic
            current_subs = DefWebhookSubscription.query.filter_by(webhook_id=webhook_id).all()
            current_event_map = {s.event_id: s for s in current_subs}
            current_event_ids = set(current_event_map.keys())
            target_event_ids = set(new_event_ids)

            to_add = target_event_ids - current_event_ids
            to_remove = current_event_ids - target_event_ids

            for event_id in to_remove:
                db.session.delete(current_event_map[event_id])

            now = datetime.utcnow()
            for event_id in to_add:
                new_sub = DefWebhookSubscription(
                    tenant_id=tenant_id,
                    webhook_id=webhook_id,
                    event_id=event_id,
                    created_by=current_user_id,
                    creation_date=now,
                    last_updated_by=current_user_id,
                    last_update_date=now
                )
                db.session.add(new_sub)

            db.session.commit()
            return make_response(jsonify({
                'message': 'Edited successfully',
                'added': list(to_add),
                'removed': list(to_remove),
                'current_total': len(target_event_ids)
            }), 200)

        return make_response(jsonify({'message': 'Provide subscription_id (query) OR webhook_id + event_ids (body)'}), 400)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating subscriptions', 'error': str(e)}), 500)


@webhooks_bp.route('/def_webhook_subscriptions', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_subscriptions():
    """Bulk delete subscriptions."""
    try:
        data = request.get_json()
        if not data or 'subscription_ids' not in data:
            return make_response(jsonify({'message': 'subscription_ids (list) is required'}), 400)

        sub_ids = data.get('subscription_ids')
        if not isinstance(sub_ids, list):
            return make_response(jsonify({'message': 'subscription_ids must be a list'}), 400)

        current_user_id = get_jwt_identity()
        user = DefUser.query.get(int(current_user_id))
        tenant_id = user.tenant_id if user else None

        query = DefWebhookSubscription.query.filter(DefWebhookSubscription.subscription_id.in_(sub_ids))
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)

        subs = query.all()
        if not subs:
            return make_response(jsonify({'message': 'No subscriptions found'}), 404)

        for sub in subs:
            db.session.delete(sub)

        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting subscriptions', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_subscriptions_v', methods=['GET'])
@jwt_required()
def get_subscriptions_v():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        webhook_id = request.args.get('webhook_id', type=int)
        webhook_name = request.args.get('webhook_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)
        
        query = DefWebhookSubscriptionV.query
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_id:
            query = query.filter_by(webhook_id=webhook_id)
        if webhook_name:
            query = query.filter(DefWebhookSubscriptionV.webhook_name.ilike(f'%{webhook_name}%'))

        if page and limit:
            paginated = query.order_by(DefWebhookSubscriptionV.webhook_id.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            return make_response(jsonify({
                'result': [s.json() for s in paginated.items],
                'total':  paginated.total,
                'pages':  paginated.pages,
                'page':   paginated.page
            }), 200)
            
        subs = query.order_by(DefWebhookSubscriptionV.webhook_id.desc()).all()
        return make_response(jsonify({'result': [s.json() for s in subs]}), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching subscription view', 'error': str(e)}), 500)
