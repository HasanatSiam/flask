from datetime import datetime
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefPrivilege
)


from utils.auth import role_required

from . import rbac_bp


@rbac_bp.route('/def_privileges', methods=['GET'])
@jwt_required()
@role_required()
def get_def_privileges():
    try:
        privilege_id = request.args.get("privilege_id", type=int)

        # Single-record lookup if privilege_id is provided
        if privilege_id is not None:
            record = DefPrivilege.query.get(privilege_id)
            if not record:
                return make_response(jsonify({
                    "error": f"Privilege with id={privilege_id} not found"
                }), 404)
            return make_response(jsonify({"result": record.json()}), 200)

        # Pagination parameters
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            # Return paginated records
            paginated = DefPrivilege.query.order_by(DefPrivilege.privilege_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [r.json() for r in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Otherwise return all records
        records = DefPrivilege.query.order_by(DefPrivilege.privilege_id.desc()).all()
        return make_response(jsonify({"result": [r.json() for r in records]}), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching privileges"
        }), 500)




@rbac_bp.route('/def_privileges', methods=['POST'])
@jwt_required()
@role_required()
def create_def_privilege():
    try:
        data = request.get_json()
        privilege_name = data.get('privilege_name')

        if not privilege_name:
            return make_response(jsonify({"error": "privilege_name is required"}), 400)

        new_record = DefPrivilege(
            privilege_name=privilege_name,
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_updated_by=get_jwt_identity(),
            last_update_date=datetime.utcnow()
        )

        db.session.add(new_record)
        db.session.commit()

        return make_response(jsonify({
            "message": "Added successfully"
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)



@rbac_bp.route('/def_privileges', methods=['PUT'])
@jwt_required()
@role_required()
def update_privilege():
    try:
        privilege_id = request.args.get("privilege_id", type=int)
        if privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'privilege_id' is required"
            }), 400)
        
        privilege_name = request.json.get('privilege_name')

        privilege = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()
        if not privilege:
            return make_response(jsonify({'error': 'Privilege not found'}), 404)

        if privilege_name:
            privilege.privilege_name = privilege_name

        privilege.last_updated_by = get_jwt_identity()
        privilege.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({
            'message': 'Edited successfully'
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error updating privilege'
        }), 500)


@rbac_bp.route('/def_privileges', methods=['DELETE'])
@jwt_required()
@role_required()
def delete_privilege():
    try:
        data = request.get_json()
        if not data or 'privilege_ids' not in data:
            return make_response(jsonify({
                "error": "Request body with 'privilege_ids' (list) is required"
            }), 400)

        privilege_ids = data.get('privilege_ids')
        if not isinstance(privilege_ids, list):
            return make_response(jsonify({'error': 'privilege_ids must be a list'}), 400)

        privileges = DefPrivilege.query.filter(DefPrivilege.privilege_id.in_(privilege_ids)).all()

        if not privileges:
            return make_response(jsonify({'error': 'No privileges found for provided IDs'}), 404)

        for privilege in privileges:
            db.session.delete(privilege)

        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error deleting privileges'
        }), 500)


