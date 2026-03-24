from flask import request, jsonify, make_response
from sqlalchemy import or_
from datetime import datetime
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
            creation_date  = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()

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
            return make_response(jsonify({"result": record.json()}), 200)

        # Build list query
        query = db.session.query(DefApiEndpointRole).join(DefApiEndpoint).join(DefRoles)

        # Single ID filters
        if api_endpoint_id is not None:
            query = query.filter(DefApiEndpointRole.api_endpoint_id == api_endpoint_id)
        if role_id is not None:
            query = query.filter(DefApiEndpointRole.role_id == role_id)

        # Search filter (similar to api_endpoints searching for endpoint name or role name)
        search_term = request.args.get('search_term', '').strip()
        if search_term:
            search_underscore = search_term.replace(' ', '_')
            search_space = search_term.replace('_', ' ')
            query = query.filter(
                or_(
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_term}%'),
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_underscore}%'),
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_space}%'),
                    DefRoles.role_name.ilike(f'%{search_term}%'),
                    DefRoles.role_name.ilike(f'%{search_underscore}%'),
                    DefRoles.role_name.ilike(f'%{search_space}%')
                )
            )

        # Ordering
        query = query.order_by(DefApiEndpointRole.creation_date.desc())

        # Pagination
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [r.json() for r in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Otherwise return all matching records
        records = query.all()
        return make_response(jsonify({"result": [r.json() for r in records]}), 200)

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
        # Enforce JSON body for bulk deletions
        data = request.get_json(silent=True)
        
        if not data or 'api_endpoint_role_data' not in data:
            return make_response(jsonify({
                "error": "JSON body with 'api_endpoint_role_data' (list of {api_endpoint_id, role_id}) is required"
            }), 400)

        mapping_data = data['api_endpoint_role_data']
        
        if not isinstance(mapping_data, list):
            # If a single object was passed, wrap it in a list
            mapping_data = [mapping_data]

        if not mapping_data:
            return make_response(jsonify({"error": "'api_endpoint_role_data' list cannot be empty"}), 400)
        
        deleted_count = 0
        for entry in mapping_data:
            eid = entry.get('api_endpoint_id')
            rid = entry.get('role_id')
            
            if eid is None or rid is None:
                continue

            # Find the record
            record = DefApiEndpointRole.query.filter_by(
                api_endpoint_id=eid,
                role_id=rid
            ).first()

            if record:
                db.session.delete(record)
                deleted_count += 1

        if deleted_count == 0:
            return make_response(jsonify({'error': 'No matching API endpoint-role mappings found for the provided data'}), 404)
            
        db.session.commit()

        return make_response(jsonify({
            "message": "Deleted successfully",
            "deleted_count": deleted_count
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error deleting API endpoint-role"
        }), 500)


