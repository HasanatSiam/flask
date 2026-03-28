from flask import request, jsonify, make_response
from sqlalchemy import or_
from datetime import datetime
from flask_jwt_extended import jwt_required, get_jwt_identity
from executors.extensions import db
from utils.auth import role_required

from executors.models import (
    DefApiEndpoint,
    DefApiEndpointRole,
    DefRoles,
    DefApiEndpointRolesV,
    DefRoleApiEndpointsV

)

from . import rbac_bp






@rbac_bp.route('/def_api_endpoint_roles/by_endpoint', methods=['GET'])
@jwt_required()
@role_required()
def get_api_endpoint_roles_view():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        query = DefApiEndpointRolesV.query

        if api_endpoint_id is not None:
            query = query.filter(DefApiEndpointRolesV.api_endpoint_id == api_endpoint_id)

        search_term = request.args.get('api_endpoint', '').strip()
        if search_term:
            search_underscore = search_term.replace(' ', '_')
            search_space = search_term.replace('_', ' ')
            query = query.filter(
                or_(
                    DefApiEndpointRolesV.api_endpoint.ilike(f'%{search_term}%'),
                    DefApiEndpointRolesV.api_endpoint.ilike(f'%{search_underscore}%'),
                    DefApiEndpointRolesV.api_endpoint.ilike(f'%{search_space}%')
                )
            )

        query = query.order_by(DefApiEndpointRolesV.api_endpoint_id.desc())

        records = query.all()
        result = [r.json() for r in records]

        if api_endpoint_id is not None:
            if not result:
                return make_response(jsonify({
                    "error": f"API endpoint with id={api_endpoint_id} not found"
                }), 404)
            return make_response(jsonify({"result": result[0]}), 200)

        return make_response(jsonify({"result": result}), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching API endpoint roles view"
        }), 500)



@rbac_bp.route('/def_api_endpoint_roles', methods=['POST'])
@jwt_required()
@role_required()
def create_api_endpoint_role():
    try:
        data = request.json
        api_endpoint_id = data.get('api_endpoint_id')
        role_ids = data.get('role_ids')

        # Validation
        if api_endpoint_id is None or role_ids is None or not isinstance(role_ids, list):
            return make_response(jsonify({
                "error": "api_endpoint_id (int) and role_ids (list) are required"
            }), 400)

        # 1. FK Check: API Endpoint
        endpoint = DefApiEndpoint.query.get(api_endpoint_id)
        if not endpoint:
            return make_response(jsonify({
                "error": f"API Endpoint ID {api_endpoint_id} does not exist"
            }), 404)

        # 2. FK Check: Roles
        valid_roles = DefRoles.query.filter(DefRoles.role_id.in_(role_ids)).all()
        found_role_ids = {r.role_id for r in valid_roles}
        missing_role_ids = list(set(role_ids) - found_role_ids)
        
        if missing_role_ids:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_role_ids": missing_role_ids
            }), 404)

        # 3. Filter out existing mappings (Duplicate Prevention)
        existing_mappings = DefApiEndpointRole.query.filter_by(api_endpoint_id=api_endpoint_id).all()
        existing_role_ids = {m.role_id for m in existing_mappings}
        
        roles_to_add = set(role_ids) - existing_role_ids

        if not roles_to_add:
            return make_response(jsonify({
                "message": "All provided role mappings already exist",
                "added_count": 0
            }), 200)

        # 4. Batch Create
        identity = get_jwt_identity()
        now = datetime.utcnow()
        for rid in roles_to_add:
            new_mapping = DefApiEndpointRole(
                api_endpoint_id=api_endpoint_id,
                role_id=rid,
                created_by=identity,
                creation_date=now,
                last_updated_by=identity,
                last_update_date=now
            )
            db.session.add(new_mapping)

        db.session.commit()

        return make_response(jsonify({
            "message": "Added successfully"
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating API endpoint roles"
        }), 500)


@rbac_bp.route('/def_api_endpoint_roles', methods=['GET'])
@jwt_required()
@role_required()
def get_api_endpoint_roles():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Support single and multiple role IDs (e.g., ?role_id=1,2 or ?role_id=1&role_id=2)
        role_ids_raw = request.args.getlist("role_id")
        role_ids = []
        for val in role_ids_raw:
            for item in val.split(','):
                item = item.strip()
                if item.isdigit():
                    role_ids.append(int(item))
        role_ids = sorted(list(set(role_ids)))

        # If exactly one endpoint and one role provided -> single-record lookup
        if api_endpoint_id is not None and len(role_ids) == 1:
            record = DefApiEndpointRole.query.filter_by(
                api_endpoint_id=api_endpoint_id,
                role_id=role_ids[0]
            ).first()
            if not record:
                return make_response(jsonify({
                    "error": f"No data found for api_endpoint_id={api_endpoint_id} and role_id={role_ids[0]}"
                }), 404)
            return make_response(jsonify({"result": record.json()}), 200)

        # Build list query
        query = db.session.query(DefApiEndpointRole).join(DefApiEndpoint).join(DefRoles)

        # Single ID filters
        if api_endpoint_id is not None:
            query = query.filter(DefApiEndpointRole.api_endpoint_id == api_endpoint_id)
        if role_ids:
            query = query.filter(DefApiEndpointRole.role_id.in_(role_ids))

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
@role_required()
def update_api_endpoint_role():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        if api_endpoint_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'api_endpoint_id' is required"
            }), 400)

        data = request.json
        new_role_ids = data.get("role_ids")

        if new_role_ids is None or not isinstance(new_role_ids, list):
            return make_response(jsonify({
                "error": "Body parameter 'role_ids' (list) is required"
            }), 400)

        # 1. Validate Endpoint exists
        endpoint = DefApiEndpoint.query.get(api_endpoint_id)
        if not endpoint:
            return make_response(jsonify({"error": f"API Endpoint ID {api_endpoint_id} not found"}), 404)

        # 2. Validate all new Roles exist
        valid_roles = DefRoles.query.filter(DefRoles.role_id.in_(new_role_ids)).all()
        found_role_ids = {r.role_id for r in valid_roles}
        missing_role_ids = list(set(new_role_ids) - found_role_ids)
        
        if missing_role_ids:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_role_ids": missing_role_ids
            }), 404)

        # 3. Synchronize Mappings
        current_mappings = DefApiEndpointRole.query.filter_by(api_endpoint_id=api_endpoint_id).all()
        current_role_ids = {m.role_id for m in current_mappings}
        
        incoming_role_ids = set(new_role_ids)
        
        roles_to_add = incoming_role_ids - current_role_ids
        roles_to_remove = current_role_ids - incoming_role_ids

        # Delete removed roles
        if roles_to_remove:
            DefApiEndpointRole.query.filter(
                DefApiEndpointRole.api_endpoint_id == api_endpoint_id,
                DefApiEndpointRole.role_id.in_(list(roles_to_remove))
            ).delete(synchronize_session=False)

        # Add new roles
        identity = get_jwt_identity()
        now = datetime.utcnow()
        for rid in roles_to_add:
            new_mapping = DefApiEndpointRole(
                api_endpoint_id=api_endpoint_id,
                role_id=rid,
                created_by=identity,
                creation_date=now,
                last_updated_by=identity,
                last_update_date=now
            )
            db.session.add(new_mapping)

        db.session.commit()

        # Fetch final state for response
        final_state = DefApiEndpointRole.query.filter_by(api_endpoint_id=api_endpoint_id).all()

        return make_response(jsonify({
            "message": "Edited successfully",
            "api_endpoint_id": api_endpoint_id,
            "role_ids": [m.role_id for m in final_state]
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating API endpoint-role mapping"
        }), 500)



@rbac_bp.route('/def_api_endpoint_roles', methods=['DELETE'])
@jwt_required()
@role_required()
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


