from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefControlEnvironment
)

from . import controls_bp




@controls_bp.route('/def_control_environments', methods=['GET'])
@jwt_required()
def get_control_environments():
    try:
        # Query params
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)
        control_environment_id = request.args.get("control_environment_id", type=int)
        name = request.args.get("name", "").strip()

        # Validate pagination
        if page is not None and limit is not None:
            if page < 1 or limit < 1:
                return make_response(jsonify({
                    "message": "Page and limit must be positive integers"
                }), 400)

        # Base query
        query = DefControlEnvironment.query

        # Filter by control_environment_id
        if control_environment_id:
            query = query.filter(DefControlEnvironment.control_environment_id == control_environment_id)

        # Filter by name (supports underscores/spaces)
        if name:
            search_underscore = name.replace(" ", "_")
            search_space = name.replace("_", " ")
            query = query.filter(
                or_(
                    DefControlEnvironment.name.ilike(f"%{name}%"),
                    DefControlEnvironment.name.ilike(f"%{search_underscore}%"),
                    DefControlEnvironment.name.ilike(f"%{search_space}%"),
                )
            )

        # Order by latest first
        query = query.order_by(DefControlEnvironment.control_environment_id.desc())

        # Paginated response
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            items = [env.json() for env in paginated.items]
            return make_response(jsonify({
                "items": items,
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Return all if no pagination
        environments = query.all()
        if environments:
            return make_response(jsonify([env.json() for env in environments]), 200)
        else:
            return make_response(jsonify({"message": "No control environments found"}), 404)

    except Exception as e:
        return make_response(
            jsonify({
                "message": "Error retrieving control environments",
                "error": str(e)
            }),
            500
        )




@controls_bp.route('/def_control_environments', methods=['POST'])
@jwt_required()
def create_control_environment():
    try:
        data = request.get_json()
        current_user = get_jwt_identity()

        if not data or "name" not in data:
            return make_response(jsonify({"message": "Missing required field: name"}), 400)

        new_env = DefControlEnvironment(
            name=data.get("name"),
            description=data.get("description"),
            created_by=current_user,
            last_updated_by=current_user,
        )

        db.session.add(new_env)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully",
                                      "result": new_env.json() }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(
            jsonify({
                "message": "Error creating control environment",
                "error": str(e)
            }),
            500
        )




@controls_bp.route('/def_control_environments', methods=['PUT'])
@jwt_required()
def update_control_environment():
    try:
        control_environment_id = request.args.get('control_environment_id', type=int)
        if not control_environment_id:
            return make_response(jsonify({"message": "Control Environment ID is required"}), 400)

        data = request.get_json()


        env = DefControlEnvironment.query.filter_by(control_environment_id=control_environment_id).first()
        if not env:
            return make_response(jsonify({"message": "Control environment not found"}), 404)


        if env:
            env.name = data.get("name", env.name)
            env.description = data.get("description", env.description)
            env.last_updated_by = get_jwt_identity()



            db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "result": env.json()
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Error updating control environment",
            "error": str(e)
        }), 500)



@controls_bp.route('/def_control_environments', methods=['DELETE'])
@jwt_required()
def delete_control_environments():
    try:
        data = request.get_json()
        environment_ids = data.get("control_environment_ids", []) if data else []

        if not environment_ids:
            return make_response(jsonify({"message": "Environment IDs are required"}), 400)

        envs = DefControlEnvironment.query.filter(
            DefControlEnvironment.control_environment_id.in_(environment_ids)
        ).all()

        if not envs:
            return make_response(jsonify({"message": "No matching control environments found"}), 404)

        for env in envs:
            db.session.delete(env)

        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error deleting control environments", "error": str(e)}), 500)

