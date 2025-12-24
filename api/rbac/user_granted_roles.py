from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db

from executors.models import (
    DefUser,
    DefRoles,
    DefUserGrantedRole
)

from utils.auth import role_required
from . import rbac_bp






@rbac_bp.route('/def_user_granted_roles', methods=['POST'])
@jwt_required()
def create_user_granted_roles():
    try:
        data = request.json
        user_id = data.get('user_id')
        role_ids = data.get('role_ids')

        # Validate input
        if not user_id or not role_ids or not isinstance(role_ids, list):
            return make_response(jsonify({"error": "user_id and role_ids (list) are required"}), 400)

        # Check if user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)


        # 1. Check for duplicates in one query
        existing_roles = DefUserGrantedRole.query.filter(
            DefUserGrantedRole.user_id == user_id,
            DefUserGrantedRole.role_id.in_(role_ids)
        ).all()
        duplicate_role_ids = [r.role_id for r in existing_roles]

        if duplicate_role_ids:
            return make_response(jsonify({
                "error": "Some roles are already assigned to the user",
                "duplicate_roles": duplicate_role_ids
            }), 409)

        # 2. Fetch all roles in one query to ensure they exist
        roles = DefRoles.query.filter(DefRoles.role_id.in_(role_ids)).all()
        found_role_ids = [r.role_id for r in roles]
        missing_role_ids = list(set(role_ids) - set(found_role_ids))

        if missing_role_ids:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_roles": missing_role_ids
            }), 404)

        # 3. Create new mappings
        new_mappings = []
        for role in roles:
            new_mapping = DefUserGrantedRole(
                user_id=user_id,
                role_id=role.role_id,
                created_by=get_jwt_identity(),
                creation_date=datetime.utcnow(),
                last_updated_by=get_jwt_identity(),
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_mapping)
            new_mappings.append(new_mapping)

        db.session.commit()

        # Return response with success message
        return make_response(jsonify({
            "message": "Roles assigned successfully",
            "assigned_roles": [m.json() for m in new_mappings]
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating user-role mappings"
        }), 500)



@rbac_bp.route('/def_user_granted_roles', methods=['GET'])
@jwt_required()
def get_user_granted_roles():
    try:
        user_id = request.args.get('user_id', type=int)
        role_id = request.args.get('role_id', type=int)

        query = DefUserGrantedRole.query

        # Filter by user_id if provided
        if user_id:
            query = query.filter_by(user_id=user_id)

        # Filter by role_id if provided
        if role_id:
            query = query.filter_by(role_id=role_id)

        results = query.all()

        # If both params given and no record found → return 404
        if user_id and role_id and not results:
            return make_response(jsonify({
                "error": f"No mapping found for user_id={user_id} and role_id={role_id}"
            }), 404)

        return make_response(jsonify([m.json() for m in results]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching user-role mappings"
        }), 500)



@rbac_bp.route('/def_user_granted_roles', methods=['PUT'])
@jwt_required()
def update_user_granted_roles():
    try:
        # user_id from query params
        user_id = request.args.get("user_id", type=int)
        if not user_id:
            return make_response(jsonify({"error": "user_id is required"}), 400)

        data = request.json
        role_ids = data.get("role_ids")

        # Validate role_ids
        if not role_ids or not isinstance(role_ids, list):
            return make_response(jsonify({"error": "role_ids (list) is required"}), 400)

        # Check user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # Fetch existing roles
        existing = DefUserGrantedRole.query.filter_by(user_id=user_id).all()
        existing_role_ids = {r.role_id for r in existing}

        incoming_role_ids = set(map(int, role_ids))

        # Determine differences
        to_add = incoming_role_ids - existing_role_ids
        to_remove = existing_role_ids - incoming_role_ids

        # Validate that incoming roles exist
        valid_roles = DefRoles.query.filter(
            DefRoles.role_id.in_(incoming_role_ids)
        ).all()
        found_ids = {r.role_id for r in valid_roles}

        missing = incoming_role_ids - found_ids
        if missing:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_role_ids": list(missing)
            }), 404)

        # Delete removed roles
        if to_remove:
            DefUserGrantedRole.query.filter(
                DefUserGrantedRole.user_id == user_id,
                DefUserGrantedRole.role_id.in_(to_remove)
            ).delete(synchronize_session=False)

        # Add new roles
        for rid in to_add:
            db.session.add(
                DefUserGrantedRole(
                    user_id=user_id,
                    role_id=rid,
                    created_by=current_user,
                    creation_date=now,
                    last_updated_by=current_user,
                    last_update_date=now
                )
            )

        # Update audit fields for kept roles
        for rid in incoming_role_ids & existing_role_ids:
            mapping = DefUserGrantedRole.query.filter_by(
                user_id=user_id, role_id=rid
            ).first()
            mapping.last_updated_by = current_user
            mapping.last_update_date = now

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "role_ids": sorted(list(incoming_role_ids))
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating user-role mappings"
        }), 500)




@rbac_bp.route('/def_user_granted_roles', methods=['DELETE'])
@jwt_required()
def delete_user_granted_role():
    try:

        # Extract query parameters
        user_id = request.args.get("user_id", type=int)
        role_id = request.args.get("role_id", type=int)

        if not user_id or not role_id:
            return make_response(jsonify({
                "error": "Missing required query parameters: user_id, role_id"
            }), 400)
        
        # Find the mapping
        mapping = DefUserGrantedRole.query.filter_by(
            user_id=user_id,
            role_id=role_id
        ).first()

        if not mapping:
            return make_response(jsonify({
                "error": f"Mapping for user_id={user_id} and role_id={role_id} not found"
            }), 404)

        # Set who performed the delete (audit tracking)
        mapping.last_updated_by = get_jwt_identity()

        db.session.delete(mapping)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error deleting user granted role"
        }), 500)


