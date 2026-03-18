from flask import request, jsonify, make_response
from sqlalchemy import or_
from datetime import datetime
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefPrivilege,
    DefApiEndpoint,
    DefApiEndpointRole
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
            return make_response(jsonify({"result": endpoint.json()}), 200)

        # Base query
        query = DefApiEndpoint.query

        # Search filter
        search_term = request.args.get('api_endpoint', '').strip()
        if search_term:
            search_underscore = search_term.replace(' ', '_')
            search_space = search_term.replace('_', ' ')
            query = query.filter(
                or_(
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_term}%'),
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_underscore}%'),
                    DefApiEndpoint.api_endpoint.ilike(f'%{search_space}%')
                )
            )

        # Ordering
        query = query.order_by(DefApiEndpoint.api_endpoint_id.desc())

        # Pagination
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [e.json() for e in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Otherwise return all endpoints
        endpoints = query.all()
        return make_response(jsonify({"result": [e.json() for e in endpoints]}), 200)

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
        # Enforce JSON body for bulk deletions
        data = request.get_json(silent=True)
        
        if not data or 'api_endpoint_ids' not in data:
            return make_response(jsonify({
                "error": "JSON body with 'api_endpoint_ids' (list of IDs) is required"
            }), 400)

        ids = data['api_endpoint_ids']
        
        if not isinstance(ids, list):
            # If a single ID was passed instead of a list, wrap it in a list
            ids = [ids]

        if not ids:
            return make_response(jsonify({"error": "'api_endpoint_ids' list cannot be empty"}), 400)
        
        # 1. DELETE FROM def_api_endpoint_roles first to avoid ForeignKeyViolation
        DefApiEndpointRole.query.filter(DefApiEndpointRole.api_endpoint_id.in_(ids)).delete(synchronize_session=False)

        # 2. Get and delete the actual endpoints
        rows = DefApiEndpoint.query.filter(DefApiEndpoint.api_endpoint_id.in_(ids)).all()
        
        if not rows:
            # If we don't find any endpoints, we should still commit the role deletion just in case
            db.session.commit()
            return make_response(jsonify({'error': 'No matching API endpoints found for the provided ID(s)'}), 404)

        deleted_count = 0
        for row in rows:
            db.session.delete(row)
            deleted_count += 1
            
        db.session.commit()

        return make_response(jsonify({
            'message': 'Deleted successfully',
            'deleted_count': deleted_count
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)


