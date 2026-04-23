from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required

from utils.auth import role_required
from executors.models import DefWebhook, LogWebhookDelivery

from . import webhooks_bp


# ── GET /log_webhook_deliveries ───────────────────────────────────────────────

@webhooks_bp.route('/log_webhook_deliveries', methods=['GET'])
@jwt_required()
# @role_required()
def get_webhook_deliveries():
    try:
        webhook_id      = request.args.get('webhook_id',      type=int)
        tenant_id       = request.args.get('tenant_id',       type=int)
        webhook_name    = request.args.get('webhook_name',    type=str)
        delivery_status = request.args.get('delivery_status', type=str)
        page            = request.args.get('page',            type=int)
        limit           = request.args.get('limit',           type=int)

        query = LogWebhookDelivery.query.join(
            DefWebhook, LogWebhookDelivery.webhook_id == DefWebhook.webhook_id
        )

        if webhook_id:
            query = query.filter_by(webhook_id=webhook_id)
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)
        if webhook_name:
            query = query.filter(DefWebhook.webhook_name.ilike(f'%{webhook_name}%'))
        if delivery_status:
            query = query.filter_by(delivery_status=delivery_status.upper())

        # Paginated
        if page and limit:
            paginated = query.order_by(LogWebhookDelivery.delivery_id.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            return make_response(jsonify({
                'result': [d.json() for d in paginated.items],
                'total':  paginated.total,
                'pages':  1 if paginated.total == 0 else paginated.pages,
                'page':   paginated.page
            }), 200)

        deliveries = query.order_by(LogWebhookDelivery.delivery_id.desc()).all()
        return make_response(jsonify({'result': [d.json() for d in deliveries]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching deliveries', 'error': str(e)}), 500)
