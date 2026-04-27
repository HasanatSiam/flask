# api/webhooks/logs.py
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required

from executors.extensions import db
from executors.models import LogWebhookDelivery, DefWebhook, DefWebhookEvent
from . import webhooks_bp

@webhooks_bp.route('/log_webhook_deliveries', methods=['GET'])
@jwt_required()
# @role_required()
def get_webhook_deliveries():
    try:
        tenant_id    = request.args.get('tenant_id',    type=int)
        webhook_name = request.args.get('webhook_name', type=str)
        event_name   = request.args.get('event_name',   type=str)
        page         = request.args.get('page',         type=int, default=1)
        limit        = request.args.get('limit',        type=int, default=20)

        query = db.session.query(
            LogWebhookDelivery,
            DefWebhook.webhook_name,
            DefWebhookEvent.event_name
        ).outerjoin(
            DefWebhook,
            LogWebhookDelivery.webhook_id == DefWebhook.webhook_id
        ).outerjoin(
            DefWebhookEvent,
            LogWebhookDelivery.event_id == DefWebhookEvent.event_id
        )

        if tenant_id:
            query = query.filter(LogWebhookDelivery.tenant_id == tenant_id)
        if webhook_name:
            query = query.filter(DefWebhook.webhook_name.ilike(f'%{webhook_name}%'))
        if event_name:
            query = query.filter(DefWebhookEvent.event_name.ilike(f'%{event_name}%'))

        paginated = query.order_by(
            LogWebhookDelivery.delivery_id.desc()
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
        return make_response(jsonify({'message': 'Error fetching delivery logs', 'error': str(e)}), 500)
