from datetime import datetime
from flask import request, jsonify, make_response     

from flask_jwt_extended import jwt_required, get_jwt_identity
from executors.extensions import db
from utils.auth import role_required

from executors.models import (
    DefApiEndpoint,
    DefApiEndpointRole,
    DefRoles

)

from . import rbac_bp




@rbac_bp.route('/def_api_endpoint_roles', methods=['POST'])
@jwt_required()
def create_api_endpoint_role():
    try:
        api_endpoint_id = request.json.get('api_endpoint_id')
        role_id = request.json.get('role_id')


        # Validation
        if not api_endpoint_id or not role_id:
            return make_response(jsonify({
                "error": "api_endpoint_id and role_id are required"
            }), 400)

        # FK Check: API Endpoint
        endpoint = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not endpoint:
            return make_response(jsonify({
                "error": f"API Endpoint ID {api_endpoint_id} does not exist"
            }), 404)

        # FK Check: Role
        role = DefRoles.query.filter_by(role_id=role_id).first()
        if not role:
            return make_response(jsonify({
                "error": f"Role ID {role_id} does not exist"
            }), 404)

        # Check duplicate
        existing = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()
        if existing:
            return make_response(jsonify({
                "error": "Mapping already exists"
            }), 409)

        new_mapping = DefApiEndpointRole(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow()

        )

        db.session.add(new_mapping)
        db.session.commit()

        return make_response(jsonify(new_mapping.json()), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating API endpoint role"
        }), 500)


@rbac_bp.route('/def_api_endpoint_roles', methods=['GET'])
@jwt_required()
def get_api_endpoint_roles():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # If both provided -> single-record lookup
        if api_endpoint_id is not None and role_id is not None:
            record = DefApiEndpointRole.query.filter_by(
                api_endpoint_id=api_endpoint_id,
                role_id=role_id
            ).first()
            if not record:
                return make_response(jsonify({
                    "error": f"No data found for api_endpoint_id={api_endpoint_id} and role_id={role_id}"
                }), 404)
            return make_response(jsonify(record.json()), 200)

        # Otherwise build a list query (may be empty)
        query = DefApiEndpointRole.query

        if api_endpoint_id is not None:
            query = query.filter_by(api_endpoint_id=api_endpoint_id)
        if role_id is not None:
            query = query.filter_by(role_id=role_id)

        records = query.order_by(DefApiEndpointRole.creation_date.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching API endpoint roles"
        }), 500)





@rbac_bp.route('/def_api_endpoint_roles', methods=['PUT'])
@jwt_required()
def update_api_endpoint_role():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # Validate required params
        if api_endpoint_id is None or role_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'api_endpoint_id' and 'role_id' are required"
            }), 400)

        record = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()

        if not record:
            return make_response(jsonify({
                "error": "Record not found",
                "message": "API Endpoint-Role mapping does not exist"
            }), 404)
        
        record.api_endpoint_id = request.json.get('api_endpoint_id', record.api_endpoint_id)
        record.role_id = request.json.get('role_id', record.role_id)

        record.last_updated_by = get_jwt_identity()
        record.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "data": record.json()
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating API endpoint-role mapping"
        }), 500)



@rbac_bp.route('/def_api_endpoint_roles', methods=['DELETE'])
@jwt_required()
def delete_api_endpoint_role():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # Validate required params
        if api_endpoint_id is None or role_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'api_endpoint_id' and 'role_id' are required"
            }), 400)

        # Find the record
        record = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()

        if not record:
            return make_response(jsonify({
                "error": f"No mapping found for api_endpoint_id={api_endpoint_id} and role_id={role_id}"
            }), 404)

        db.session.delete(record)
        db.session.commit()

        return make_response(jsonify({
            "message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error deleting API endpoint-role"
        }), 500)


