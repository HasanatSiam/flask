from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefWebhookEvent, DefApiEndpoint, DefPrivilege, DefUser
from . import webhooks_bp

@webhooks_bp.route('/def_webhook_events', methods=['GET'])
@jwt_required()
# @role_required()
def get_webhook_events():
    try:
        event_id = request.args.get('event_id', type=int)
        tenant_id = request.args.get('tenant_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        query = db.session.query(
            DefWebhookEvent,
            DefApiEndpoint.api_endpoint,
            DefApiEndpoint.method
        ).join(
            DefApiEndpoint, DefWebhookEvent.api_endpoint_id == DefApiEndpoint.api_endpoint_id
        )

        if tenant_id:
            query = query.filter(DefWebhookEvent.tenant_id == tenant_id)
        if event_id:
            query = query.filter(DefWebhookEvent.event_id == event_id)

        # Pagination logic
        total = query.count()
        if page and limit:
            offset = (page - 1) * limit
            results = query.offset(offset).limit(limit).all()
        else:
            results = query.all()

        catalog = []
        for event, path, method in results:
            item = event.json()
            item['technical_details'] = {
                'path': path,
                'method': method
            }
            catalog.append(item)

        response = {'result': catalog}
        if page and limit:
            response.update({
                'total': total,
                'page': page,
                'limit': limit,
                'pages': (total + limit - 1) // limit
            })

        return make_response(jsonify(response), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching event catalog', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_events', methods=['POST'])
@jwt_required()
# @role_required()
def register_business_event():
    """Manual registration of a business event linked to a technical endpoint."""
    try:
        data = request.get_json()
        current_user_id = get_jwt_identity()
        user = DefUser.query.get(int(current_user_id))
        tenant_id = user.tenant_id if user else None

        new_event = DefWebhookEvent(
            tenant_id       = tenant_id,
            api_endpoint_id = data.get('api_endpoint_id'),
            event_name      = data.get('event_name'),
            event_key       = data.get('event_key'),
            description     = data.get('description'),
            created_by      = current_user_id,
            creation_date   = datetime.utcnow(),
            last_update_date = datetime.utcnow(),
            last_updated_by = current_user_id,
        )
        db.session.add(new_event)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully', 'result': new_event.json()}), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error registering event', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_events', methods=['PUT'])
@jwt_required()
# @role_required()
def update_business_event():
    """Update an existing business event."""
    try:
        event_id = request.args.get('event_id', type=int)
        if not event_id:
            return make_response(jsonify({'message': 'event_id is required as a query parameter'}), 400)

        event = DefWebhookEvent.query.get(event_id)
        if not event:
            return make_response(jsonify({'message': 'Business event not found'}), 404)

        data = request.get_json()
        
        # Update allowed fields
        if 'api_endpoint_id' in data:
            event.api_endpoint_id = data.get('api_endpoint_id')
        if 'event_name' in data:
            event.event_name = data.get('event_name')
        if 'event_key' in data:
            event.event_key = data.get('event_key')
        if 'description' in data:
            event.description = data.get('description')
            
        event.last_updated_by = get_jwt_identity()
        event.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully', 'result': event.json()}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating event', 'error': str(e)}), 500)

@webhooks_bp.route('/def_webhook_events', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_business_events():
    """Delete multiple business events at once."""
    try:
        data = request.get_json()
        if not data or 'event_ids' not in data:
            return make_response(jsonify({'message': 'event_ids (list) is required in the JSON body'}), 400)

        event_ids = data.get('event_ids')
        if not isinstance(event_ids, list):
            return make_response(jsonify({'message': 'event_ids must be a list of integers'}), 400)

        events = DefWebhookEvent.query.filter(DefWebhookEvent.event_id.in_(event_ids)).all()
        
        if not events:
            return make_response(jsonify({'message': 'No business events found for the provided IDs'}), 404)

        for event in events:
            db.session.delete(event)

        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting events', 'error': str(e)}), 500)
