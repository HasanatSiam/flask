from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefWebhook

from . import webhooks_bp


# ── GET /def_webhooks ────────────────────────────────────────────────────────

@webhooks_bp.route('/def_webhooks', methods=['GET'])
@jwt_required()
@role_required()
def get_def_webhooks():
    try:
        webhook_id   = request.args.get('webhook_id',   type=int)
        tenant_id    = request.args.get('tenant_id',    type=int)
        webhook_name = request.args.get('webhook_name', type=str)
        table_name   = request.args.get('table_name',   type=str)
        is_active    = request.args.get('is_active',    type=str)
        page         = request.args.get('page',         type=int)
        limit        = request.args.get('limit',        type=int)

        # Case 1: Get single by ID
        if webhook_id:
            webhook = DefWebhook.query.filter_by(webhook_id=webhook_id).first()
            if webhook:
                return make_response(jsonify({'result': webhook.json()}), 200)
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        query = DefWebhook.query

        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_name:
            query = query.filter(DefWebhook.webhook_name.ilike(f'%{webhook_name}%'))
        if table_name:
            query = query.filter(DefWebhook.table_name.ilike(f'%{table_name}%'))
        if is_active:
            query = query.filter_by(is_active=is_active.upper())

        # Case 2: Paginated
        if page and limit:
            paginated = query.order_by(DefWebhook.webhook_id.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            return make_response(jsonify({
                'result': [w.json() for w in paginated.items],
                'total':  paginated.total,
                'pages':  1 if paginated.total == 0 else paginated.pages,
                'page':   paginated.page
            }), 200)

        # Case 3: Get all
        webhooks = query.order_by(DefWebhook.webhook_id.desc()).all()
        return make_response(jsonify({'result': [w.json() for w in webhooks]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching webhooks', 'error': str(e)}), 500)


# ── POST /def_webhooks ───────────────────────────────────────────────────────

@webhooks_bp.route('/def_webhooks', methods=['POST'])
@jwt_required()
@role_required()
def create_webhook():
    try:
        data = request.get_json()

        webhook_name = data.get('webhook_name')
        webhook_url  = data.get('webhook_url')
        table_name   = data.get('table_name')
        tenant_id    = data.get('tenant_id')

        if not webhook_name or not webhook_url or not table_name or not tenant_id:
            return make_response(jsonify({'message': 'webhook_name, webhook_url, table_name, and tenant_id are required'}), 400)

        new_webhook = DefWebhook(
            tenant_id        = tenant_id,
            webhook_name     = webhook_name,
            webhook_url      = webhook_url,
            table_name       = table_name,
            http_methods     = data.get('http_methods', ['POST']), # List of methods, e.g. ["POST", "PUT"]
            selected_columns = data.get('selected_columns'),      # List of columns to include
            secret_key       = data.get('secret_key'),
            extra_headers    = data.get('extra_headers'),
            filters          = data.get('filters'),
            is_active        = data.get('is_active', 'Y').upper(),
            failure_count    = 0,
            max_retries      = data.get('max_retries', 5),
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
        )
        db.session.add(new_webhook)
        db.session.commit()
        return make_response(jsonify({'message': 'Webhook created successfully', 'result': new_webhook.json()}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating webhook', 'error': str(e)}), 500)


# ── PUT /def_webhooks ────────────────────────────────────────────────────────

@webhooks_bp.route('/def_webhooks', methods=['PUT'])
@jwt_required()
@role_required()
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
        webhook.table_name       = data.get('table_name',       webhook.table_name)
        webhook.http_methods     = data.get('http_methods',     webhook.http_methods)
        webhook.selected_columns = data.get('selected_columns', webhook.selected_columns)
        webhook.secret_key       = data.get('secret_key',       webhook.secret_key)
        webhook.is_active        = data.get('is_active',        webhook.is_active)
        webhook.max_retries      = data.get('max_retries',      webhook.max_retries)
        webhook.extra_headers    = data.get('extra_headers',    webhook.extra_headers)
        webhook.filters          = data.get('filters',          webhook.filters)
        webhook.last_updated_by   = get_jwt_identity()
        webhook.last_update_date  = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Webhook updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating webhook', 'error': str(e)}), 500)


# ── DELETE /def_webhooks ─────────────────────────────────────────────────────

@webhooks_bp.route('/def_webhooks', methods=['DELETE'])
@jwt_required()
@role_required()
def delete_webhook():
    try:
        webhook_id = request.args.get('webhook_id', type=int)
        if not webhook_id:
            return make_response(jsonify({'message': 'webhook_id is required'}), 400)

        webhook = DefWebhook.query.filter_by(webhook_id=webhook_id).first()
        if not webhook:
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        db.session.delete(webhook)
        db.session.commit()
        return make_response(jsonify({'message': 'Webhook deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting webhook', 'error': str(e)}), 500)


# ── PATCH /def_webhooks/toggle ───────────────────────────────────────────────
# Quickly activate or deactivate a webhook

@webhooks_bp.route('/def_webhooks/toggle', methods=['PATCH'])
@jwt_required()
@role_required()
def toggle_webhook():
    try:
        webhook_id = request.args.get('webhook_id', type=int)
        if not webhook_id:
            return make_response(jsonify({'message': 'webhook_id is required'}), 400)

        webhook = DefWebhook.query.filter_by(webhook_id=webhook_id).first()
        if not webhook:
            return make_response(jsonify({'message': 'Webhook not found'}), 404)

        webhook.is_active        = 'N' if webhook.is_active == 'Y' else 'Y'
        webhook.failure_count    = 0   # reset failure count on re-activation
        webhook.last_updated_by  = get_jwt_identity()
        webhook.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({
            'message':   f"Webhook {'activated' if webhook.is_active == 'Y' else 'deactivated'} successfully",
            'is_active': webhook.is_active
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error toggling webhook', 'error': str(e)}), 500)
