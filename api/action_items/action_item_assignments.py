from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_, func
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefActionItemAssignment,
    DefActionItemsV,

)
from . import action_items_bp


# Create DefActionItemAssignments (multiple user_ids)
@action_items_bp.route('/def_action_item_assignments', methods=['POST'])
@jwt_required()
def create_action_item_assignments():
    try:
        action_item_id = request.json.get('action_item_id')
        user_ids = request.json.get('user_ids')
        status = request.json.get('status')


        if not action_item_id:
            return make_response(jsonify({"message": "action_item_id is required"}), 400)
        if not user_ids or not isinstance(user_ids, list):
            return make_response(jsonify({"message": "user_ids must be a non-empty list"}), 400)

        created_assignments = []
        for uid in user_ids:
            assignment = DefActionItemAssignment(
                action_item_id = action_item_id,
                user_id = uid,
                status = status,
                created_by = get_jwt_identity(),
                creation_date = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )
            db.session.add(assignment)
            created_assignments.append(assignment)

        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({"message": "One or more assignments already exist"}), 400)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error creating assignments", "error": str(e)}), 500)


# Get all DefActionItemAssignments
@action_items_bp.route('/def_action_item_assignments', methods=['GET'])
@jwt_required()
def get_action_item_assignments():
    try:
        assignments = DefActionItemAssignment.query.order_by(DefActionItemAssignment.action_item_id.desc()).all()
        if assignments:
            return make_response(jsonify([a.json() for a in assignments]), 200)
        else:
            return make_response(jsonify({"message": "No assignments found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving assignments", "error": str(e)}), 500)





# Delete a single DefActionItemAssignment
@action_items_bp.route('/def_action_item_assignments/<int:user_id>/<int:action_item_id>', methods=['DELETE'])
@jwt_required()
def delete_action_item_assignment(action_item_id, user_id):
    try:
        assignment = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id,
            user_id=user_id
        ).first()
        if assignment:
            db.session.delete(assignment)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
        return make_response(jsonify({"message": "Assignment not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error deleting assignment", "error": str(e)}), 500)

