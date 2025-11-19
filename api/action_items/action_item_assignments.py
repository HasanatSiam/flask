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
from . import access_items_bp


# Create DefActionItemAssignments (multiple user_ids)
@access_items_bp.route('/def_action_item_assignments', methods=['POST'])
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
@access_items_bp.route('/def_action_item_assignments', methods=['GET'])
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



# Update DefActionItemAssignments (replace user_ids for given action_item_id)
@access_items_bp.route('/def_action_items/update_status/<int:user_id>/<int:action_item_id>', methods=['PUT'])
@jwt_required()
def update_action_item_assignment_status(user_id, action_item_id):
    try:
        data = request.get_json()
        if not data or 'status' not in data:
            return make_response(jsonify({"message": "Missing required field: status"}), 400)

        # Fetch the assignment
        assignment = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id,
            user_id=user_id
        ).first()

        if not assignment:
            return make_response(jsonify({"message": "Assignment not found"}), 404)

        # Update only the status
        assignment.status = data['status']
        assignment.last_updated_by = get_jwt_identity()
        assignment.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Status Updated Successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error updating status", "error": str(e)}), 500)



# Delete a single DefActionItemAssignment
@access_items_bp.route('/def_action_item_assignments/<int:user_id>/<int:action_item_id>', methods=['DELETE'])
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


@access_items_bp.route('/def_action_items_view/<int:user_id>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_action_items_view(user_id, page, limit):
    try:
        status = request.args.get('status')
        action_item_name = request.args.get('action_item_name', '').strip()
        search_underscore = action_item_name.replace(' ', '_')
        search_space = action_item_name.replace('_', ' ')

        # Validate pagination
        if page < 1 or limit < 1:
            return make_response(jsonify({
                "message": "Page and limit must be positive integers"
            }), 400)

        # Base 
        # query = DefActionItemsV.query.filter_by(user_id=user_id)

        # Base query: filter by user_id and only SENT status
        query = DefActionItemsV.query.filter_by(user_id=user_id).filter(
            func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
        )

        #Apply status filter if provided
        if status:
            query = query.filter(
                func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status))
            )

        if action_item_name:
            query = query.filter(
                or_(
                    DefActionItemsV.action_item_name.ilike(f'%{action_item_name}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_underscore}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_space}%')
                )
            )

        query = query.order_by(DefActionItemsV.action_item_id.desc())

        
        # paginated
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

        # Without pagination
        
        # items = query.all()
        # return make_response(jsonify({
        #     "items": [item.json() for item in items],
        #     "total": len(items)
        # }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching action items view',
            'error': str(e)
        }), 500)


@access_items_bp.route('/def_action_items_view/<int:user_id>/<string:status>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_action_items_by_status(user_id, status, page, limit):
    try:
        # Validate pagination
        if page < 1 or limit < 1:
            return make_response(jsonify({
                "message": "Page and limit must be positive integers"
            }), 400)

        # Query filtered by user_id + status (case-insensitive, trim)
        query = DefActionItemsV.query.filter(
            DefActionItemsV.user_id == user_id,
            func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status)),
            func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
        ).order_by(DefActionItemsV.action_item_id.desc())

        # Pagination
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)

