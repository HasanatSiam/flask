from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses
from datetime import datetime
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefPrivilege,
    DefApiEndpoint
)

from utils.auth import role_required

from . import rbac_bp




@rbac_bp.route('/def_api_endpoints', methods=['POST'])
@jwt_required()
def create_api_endpoint():
    try:
        api_endpoint_id = request.json.get('api_endpoint_id')
        api_endpoint = request.json.get('api_endpoint')
        parameter1 = request.json.get('parameter1')
        parameter2 = request.json.get('parameter2')
        method = request.json.get('method')
        privilege_id = request.json.get('privilege_id')


        if not api_endpoint_id:
            return make_response(jsonify({'error': 'api_endpoint_id is required'}), 400)

        if DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first():
            return make_response(jsonify({'error': 'api_endpoint_id already exists'}), 400)

        # FK validation
        if privilege_id and not DefPrivilege.query.filter_by(privilege_id=privilege_id).first():
            return make_response(jsonify({'error': 'privilege_id not found'}), 404)

        new_api = DefApiEndpoint(
            api_endpoint_id=api_endpoint_id,
            api_endpoint=api_endpoint,
            parameter1=parameter1,
            parameter2=parameter2,
            method=method,
            privilege_id=privilege_id,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_api)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)





@rbac_bp.route('/def_api_endpoints', methods=['GET'])
@jwt_required()
def get_api_endpoints():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Single-record lookup if api_endpoint_id is provided
        if api_endpoint_id is not None:
            endpoint = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
            if not endpoint:
                return make_response(jsonify({
                    "error": f"API endpoint with id={api_endpoint_id} not found"
                }), 404)
            return make_response(jsonify(endpoint.json()), 200)

        # Otherwise return all endpoints
        endpoints = DefApiEndpoint.query.order_by(DefApiEndpoint.api_endpoint_id.desc()).all()
        return make_response(jsonify([e.json() for e in endpoints]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching API endpoints"
        }), 500)



@rbac_bp.route('/def_api_endpoints', methods=['PUT'])
@jwt_required()
def update_api_endpoint():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Validate required param
        if api_endpoint_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'api_endpoint_id' is required"
            }), 400)
        
        row = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not row:
            return make_response(jsonify({'error': 'API endpoint not found'}), 404)

        row.api_endpoint = request.json.get('api_endpoint', row.api_endpoint)
        row.parameter1 = request.json.get('parameter1', row.parameter1)
        row.parameter2 = request.json.get('parameter2', row.parameter2)
        row.method = request.json.get('method', row.method)

        privilege_id = request.json.get('privilege_id', row.privilege_id)
        if privilege_id and not DefPrivilege.query.filter_by(privilege_id=privilege_id).first():
            return make_response(jsonify({'error': 'privilege_id not found'}), 404)
        row.privilege_id = privilege_id

        row.last_updated_by = get_jwt_identity()
        row.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)



@rbac_bp.route('/def_api_endpoints', methods=['DELETE'])
@jwt_required()
def delete_api_endpoint():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Validate required param
        if api_endpoint_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'api_endpoint_id' is required"
            }), 400)
        
        row = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not row:
            return make_response(jsonify({'error': 'API endpoint not found'}), 404)

        db.session.delete(row)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)


