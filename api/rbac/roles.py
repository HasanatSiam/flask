from datetime import datetime
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db

from executors.models import (
    DefRoles
)



from utils.auth import role_required
from api.rbac import rbac_bp




@rbac_bp.route('/def_roles', methods=['POST'])
@jwt_required()
def create_role():
    try:
        role_id = request.json.get('role_id')
        role_name = request.json.get('role_name')

        if not role_id:
            return make_response(jsonify({'error': 'role_id is required'}), 400)

        if not role_name:
            return make_response(jsonify({'error': 'role_name is required'}), 400)

        existing = DefRoles.query.filter_by(role_id=role_id).first()
        if existing:
            return make_response(jsonify({'error': 'role_id already exists'}), 400)

        new_role = DefRoles(
            role_id=role_id,
            role_name=role_name,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
        )

        db.session.add(new_role)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error creating role'
        }), 500)



@rbac_bp.route('/def_roles', methods=['GET'])
@jwt_required()
def get_roles():
    try:
        role_id = request.args.get("role_id", type=int)

        # Single-role lookup if role_id is provided
        if role_id is not None:
            role = DefRoles.query.filter_by(role_id=role_id).first()
            if not role:
                return make_response(jsonify({
                    "error": f"Role with id={role_id} not found"
                }), 404)
            return make_response(jsonify(role.json()), 200)

        # Otherwise return all roles
        roles = DefRoles.query.order_by(DefRoles.role_id.desc()).all()
        return make_response(jsonify([r.json() for r in roles]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching roles"
        }), 500)



@rbac_bp.route('/def_roles', methods=['PUT'])
@jwt_required()
def update_role():
    try:
        role_id = request.args.get("role_id", type=int)
        if role_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'role_id' is required"
            }), 400)
        role_name = request.json.get('role_name')

        role = DefRoles.query.filter_by(role_id=role_id).first()
        if not role:
            return make_response(jsonify({'error': 'Role not found'}), 404)

        if role_name:
            role.role_name = role_name


        role.last_updated_by = get_jwt_identity()
        role.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error updating role'
        }), 500)


@rbac_bp.route('/def_roles', methods=['DELETE'])
@jwt_required()
def delete_role():
    try:
        role_id = request.args.get("role_id", type=int)
        if role_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'role_id' is required"
            }), 400)

        role = DefRoles.query.filter_by(role_id=role_id).first()

        if not role:
            return make_response(jsonify({'error': 'Role not found'}), 404)

        db.session.delete(role)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error deleting role'
        }), 500)

