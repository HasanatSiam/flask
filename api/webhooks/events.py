# api/webhooks/events.py
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefWebhookEvent, DefApiEndpoint, DefPrivilege
from . import webhooks_bp

@webhooks_bp.route('/def_webhook_events', methods=['GET'])
@jwt_required()
@role_required()
def get_webhook_events():
    try:
        # Join with DefApiEndpoint and DefPrivilege to provide a "Better Name" experience
        results = db.session.query(
            DefWebhookEvent,
            DefApiEndpoint.api_endpoint,
            DefApiEndpoint.method,
            DefPrivilege.privilege_name
        ).join(
            DefApiEndpoint, DefWebhookEvent.api_endpoint_id == DefApiEndpoint.api_endpoint_id
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

@webhooks_bp.route('/def_webhook_events', methods=['POST'])
@jwt_required()
# @role_required()
def register_business_event():
    """Manual registration of a business event linked to a technical endpoint."""
    try:
        data = request.get_json()
        new_event = DefWebhookEvent(
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
