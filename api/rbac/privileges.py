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
            return make_response(jsonify(record.json()), 200)

        # Otherwise return all records
        records = DefPrivilege.query.order_by(DefPrivilege.privilege_id.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching privileges"
        }), 500)




@rbac_bp.route('/def_privileges', methods=['POST'])
@jwt_required()
def create_def_privilege():
    try:
        data = request.get_json()
        privilege_id = request.json.get('privilege_id')
        privilege_name = data.get('privilege_name')

        if not privilege_id:
            return make_response(jsonify({'error': 'privilege_id is required'}), 400)

        if not privilege_name:
            return make_response({"error": "privilege_name is required"}, 400)
        
        existing = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()
        if existing:
            return make_response(jsonify({'error': 'privilege_id already exists'}), 400)

        new_record = DefPrivilege(
            privilege_id=privilege_id,
            privilege_name=privilege_name,
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
        )

        db.session.add(new_record)
        db.session.commit()

        return make_response(new_record.json(), 201)

    except Exception as e:
        db.session.rollback()
        return make_response({"error": str(e)}, 500)



@rbac_bp.route('/def_privileges', methods=['PUT'])
@jwt_required()
def update_privilege():
    try:
        privilege_id = request.args.get("privilege_id", type=int)
        if privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'privilege_id' is required"
            }), 400)
        
        privilege_name = request.json.get('privilege_name')
        # updated_by = get_jwt_identity()

        privilege = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()
        if not privilege:
            return make_response(jsonify({'error': 'Privilege not found'}), 404)

        if privilege_name:
            privilege.privilege_name = privilege_name

        privilege.last_updated_by = get_jwt_identity()
        privilege.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error updating privilege'
        }), 500)


@rbac_bp.route('/def_privileges', methods=['DELETE'])
@jwt_required()
def delete_privilege():
    try:
        privilege_id = request.args.get("privilege_id", type=int)
        if privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'privilege_id' is required"
            }), 400)
        
        privilege = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()

        if not privilege:
            return make_response(jsonify({'error': 'Privilege not found'}), 404)

        db.session.delete(privilege)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error deleting privilege'
        }), 500)

