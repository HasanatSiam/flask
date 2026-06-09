from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefTaskGroup, DefTaskGroupMember, DefAsyncTask
from . import async_task_bp



@async_task_bp.route('/def_task_groups', methods=['GET'])
@jwt_required()
@role_required()
def get_task_groups():
    """
    List all task groups.
    Optional query params:
      - group_id      : filter by a specific group
      - group_name    : partial search on group name
      - page          : page number (optional, if provided enables pagination)
      - limit         : items per page (optional)
    """
    try:
        group_id      = request.args.get('group_id', type=int)
        search        = request.args.get('group_name', '').strip()
        page          = request.args.get('page', type=int)
        limit         = request.args.get('limit', type=int)

        query = DefTaskGroup.query

        if group_id:
            query = query.filter_by(group_id=group_id)
        if search:
            query = query.filter(DefTaskGroup.group_name.ilike(f'%{search}%'))

        query = query.order_by(DefTaskGroup.group_id)

        if page is not None and limit is not None:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [t.json() for t in paginated.items],
                "total":  paginated.total,
                "pages":  1 if paginated.total == 0 else paginated.pages,
                "page":   1 if paginated.total == 0 else paginated.page,
            }), 200)
        else:
            groups = query.all()
            result = [group.json() for group in groups]
            return make_response(jsonify({"result": result}), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching task groups", "error": str(e)}), 500)



@async_task_bp.route('/def_task_groups', methods=['POST'])
@jwt_required()
@role_required()
def create_task_group():
    """
    Create a new task group.
    Body: { "group_name": "...", "description": "..." }
    """
    try:
        group_name  = request.json.get('group_name', '').strip()
        description = request.json.get('description')

        if not group_name:
            return make_response(jsonify({"error": "group_name is required"}), 400)

        if DefTaskGroup.query.filter_by(group_name=group_name).first():
            return make_response(jsonify({"error": f"Group '{group_name}' already exists"}), 409)

        group = DefTaskGroup(
            group_name       = group_name,
            description      = description,
            created_by       = get_jwt_identity(),
            last_updated_by  = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_update_date = datetime.utcnow(),
        )
        db.session.add(group)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully", "group_id": group.group_id}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error creating task group", "error": str(e)}), 500)



@async_task_bp.route('/def_task_groups', methods=['PUT'])
@jwt_required()
@role_required()
def update_task_group():
    """
    Update a group's name and/or description.
    Query: ?group_id=...
    Body: { "group_name": "...", "description": "..." }
    """
    try:
        group_id = request.args.get('group_id', type=int)
        if not group_id:
            return make_response(jsonify({"error": "group_id query parameter is required"}), 400)

        group = DefTaskGroup.query.filter_by(group_id=group_id).first()
        if not group:
            return make_response(jsonify({"message": f"Group {group_id} not found"}), 404)

        if 'group_name' in request.json:
            new_name = request.json.get('group_name', '').strip()
            if not new_name:
                return make_response(jsonify({"error": "group_name cannot be empty"}), 400)
            # Check uniqueness (excluding current group)
            existing = DefTaskGroup.query.filter(
                DefTaskGroup.group_name == new_name,
                DefTaskGroup.group_id   != group_id
            ).first()
            if existing:
                return make_response(jsonify({"error": f"Group name '{new_name}' already exists"}), 409)
            group.group_name = new_name

        if 'description' in request.json:
            group.description = request.json.get('description')

        group.last_updated_by  = get_jwt_identity()
        group.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Group updated successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error updating task group", "error": str(e)}), 500)



@async_task_bp.route('/def_task_groups', methods=['DELETE'])
@jwt_required()
@role_required()
def delete_task_group():
    """
    Delete groups. Members in def_task_group_members are removed via CASCADE.
    Body: { "group_ids": [1, 2, 3] }
    """
    try:
        group_ids = request.json.get('group_ids', [])
        if not group_ids:
            return make_response(jsonify({"error": "group_ids payload is required"}), 400)

        groups = DefTaskGroup.query.filter(DefTaskGroup.group_id.in_(group_ids)).all()
        if not groups:
            return make_response(jsonify({"message": "No groups found for the provided IDs"}), 404)

        for group in groups:
            db.session.delete(group)
            
        db.session.commit()
        return make_response(jsonify({"message": "Groups deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error deleting task groups", "error": str(e)}), 500)
