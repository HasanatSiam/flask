from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db

from executors.models import (
    DefUser,
    DefPrivilege,
    DefUserGrantedPrivilege
)

from utils.auth import role_required
from . import rbac_bp



@rbac_bp.route('/def_user_granted_privileges', methods=['POST'])
@jwt_required()
def create_user_granted_privileges():
    try:
        data = request.json
        user_id = data.get('user_id')
        privilege_ids = data.get('privilege_ids')

        # Validate input
        if not user_id or not privilege_ids or not isinstance(privilege_ids, list):
            return make_response(jsonify({"error": "user_id and privilege_ids (list) are required"}), 400)

        # Check if user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # 1. Check for duplicates in one query
        existing_privileges = DefUserGrantedPrivilege.query.filter(
            DefUserGrantedPrivilege.user_id == user_id,
            DefUserGrantedPrivilege.privilege_id.in_(privilege_ids)
        ).all()
        duplicate_privilege_ids = [p.privilege_id for p in existing_privileges]

        if duplicate_privilege_ids:
            return make_response(jsonify({
                "error": "Some privileges are already assigned to the user",
                "duplicate_privileges": duplicate_privilege_ids
            }), 409)

        # 2. Fetch all privileges in one query to ensure they exist
        privileges = DefPrivilege.query.filter(DefPrivilege.privilege_id.in_(privilege_ids)).all()
        found_privilege_ids = [p.privilege_id for p in privileges]
        missing_privilege_ids = list(set(privilege_ids) - set(found_privilege_ids))

        if missing_privilege_ids:
            return make_response(jsonify({
                "error": "Some privileges do not exist",
                "missing_privileges": missing_privilege_ids
            }), 404)

        # 3. Create new mappings
        new_mappings = []
        for privilege in privileges:
            new_mapping = DefUserGrantedPrivilege(
                user_id=user_id,
                privilege_id=privilege.privilege_id,
                created_by=current_user,
                creation_date=now,
                last_updated_by=current_user,
                last_update_date=now
            )
            db.session.add(new_mapping)
            new_mappings.append(new_mapping)

        db.session.commit()

        # Return response with success message
        return make_response(jsonify({
            "message": "Privileges assigned successfully",
            "assigned_privileges": [m.json() for m in new_mappings]
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating user-privilege mappings"
        }), 500)




@rbac_bp.route('/def_user_granted_privileges', methods=['GET'])
@jwt_required()
def get_user_granted_privileges():
    try:
        user_id = request.args.get("user_id", type=int)
        privilege_id = request.args.get("privilege_id", type=int)

        # If both provided -> single-record lookup
        if user_id is not None and privilege_id is not None:
            record = DefUserGrantedPrivilege.query.filter_by(
                user_id=user_id, privilege_id=privilege_id
            ).first()
            if not record:
                return make_response(jsonify({
                    "error": f"No mapping found for user_id={user_id} and privilege_id={privilege_id}"
                }), 404)
            return make_response(jsonify(record.json()), 200)

        # Otherwise build a list query (may be empty)
        query = DefUserGrantedPrivilege.query

        if user_id is not None:
            query = query.filter_by(user_id=user_id)
        if privilege_id is not None:
            query = query.filter_by(privilege_id=privilege_id)

        records = query.order_by(DefUserGrantedPrivilege.creation_date.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e), "message": "Error fetching user-privilege mappings"}), 500)




@rbac_bp.route('/def_user_granted_privileges', methods=['PUT'])
@jwt_required()
def update_user_granted_privileges():
    try:
        # user_id comes from query param
        user_id = request.args.get("user_id", type=int)
        data = request.json
        privilege_ids = data.get("privilege_ids")

        # Validate input
        if not user_id:
            return make_response(jsonify({"error": "user_id query parameter is required"}), 400)

        if not privilege_ids or not isinstance(privilege_ids, list):
            return make_response(jsonify({
                "error": "privilege_ids (list) is required"
            }), 400)

        # Validate user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # Fetch existing privilege assignments
        existing = DefUserGrantedPrivilege.query.filter_by(
            user_id=user_id
        ).all()
        existing_priv_ids = {p.privilege_id for p in existing}

        incoming_priv_ids = set(privilege_ids)

        # Determine differences
        to_add = incoming_priv_ids - existing_priv_ids
        to_remove = existing_priv_ids - incoming_priv_ids

        # Validate incoming privileges exist
        valid_privileges = DefPrivilege.query.filter(
            DefPrivilege.privilege_id.in_(incoming_priv_ids)
        ).all()
        found_ids = {p.privilege_id for p in valid_privileges}

        missing = incoming_priv_ids - found_ids
        if missing:
            return make_response(jsonify({
                "error": "Some privileges do not exist",
                "missing_privilege_ids": list(missing)
            }), 404)

        # Remove removed privileges
        if to_remove:
            DefUserGrantedPrivilege.query.filter(
                DefUserGrantedPrivilege.user_id == user_id,
                DefUserGrantedPrivilege.privilege_id.in_(to_remove)
            ).delete(synchronize_session=False)

        # Add new privileges
        for pid in to_add:
            db.session.add(
                DefUserGrantedPrivilege(
                    user_id=user_id,
                    privilege_id=pid,
                    created_by=current_user,
                    creation_date=now,
                    last_updated_by=current_user,
                    last_update_date=now
                )
            )

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "privilege_ids": sorted(list(incoming_priv_ids))
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating user-privilege mappings"
        }), 500)




@rbac_bp.route('/def_user_granted_privileges', methods=['DELETE'])
@jwt_required()
def delete_user_granted_privilege():
    try:
        user_id = request.args.get("user_id", type=int)
        privilege_id = request.args.get("privilege_id", type=int)

        # Validate required params
        if user_id is None or privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'user_id' and 'privilege_id' are required"
            }), 400)

        record = DefUserGrantedPrivilege.query.filter_by(
            user_id=user_id,
            privilege_id=privilege_id
        ).first()

        if not record:
            return make_response(jsonify({"error": "User-privilege not found"}), 404)

        db.session.delete(record)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)




