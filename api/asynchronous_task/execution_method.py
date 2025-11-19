from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_
from utils.auth import role_required
from executors.extensions import db
from executors.models import DefAsyncExecutionMethods



from . import async_task_bp




@async_task_bp.route('/Create_ExecutionMethod', methods=['POST'])
@jwt_required()
def Create_ExecutionMethod():
    try:
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        description = request.json.get('description')

        # Validate required fields
        if not execution_method or not internal_execution_method:
            return jsonify({"error": "Missing required fields: execution_method or internal_execution_method"}), 400

        # Check if the execution method already exists
        existing_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()
        if existing_method:
            return jsonify({"error": f"Execution method '{internal_execution_method}' already exists"}), 409

        # Create a new execution method object
        new_method = DefAsyncExecutionMethods(
            execution_method = execution_method,
            internal_execution_method = internal_execution_method,
            executor = executor,
            description = description,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        # Add to session and commit
        db.session.add(new_method)
        db.session.commit()

        return jsonify({"message": "Added successfully", "data": new_method.json()}), 201

    except Exception as e:
        return jsonify({"message": "Error creating execution method", "error": str(e)}), 500



@async_task_bp.route('/Show_ExecutionMethods', methods=['GET'])
@jwt_required()
def Show_ExecutionMethods():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@async_task_bp.route('/Show_ExecutionMethods/v1', methods=['GET'])
def Show_ExecutionMethods_v1():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@async_task_bp.route('/Show_ExecutionMethods/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_execution_methods(page, limit):
    try:
        paginated = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500



@async_task_bp.route('/def_async_execution_methods/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_execution_methods(page, limit):
    try:
        search_query = request.args.get('internal_execution_method', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncExecutionMethods.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_query}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_underscore}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error searching execution methods", "error": str(e)}), 500



@async_task_bp.route('/Show_ExecutionMethod/<string:internal_execution_method>', methods=['GET'])
@jwt_required()
def Show_ExecutionMethod(internal_execution_method):
    try:
        method = db.session.query(DefAsyncExecutionMethods).filter_by(internal_execution_method=internal_execution_method).first()
        if not method:
            return jsonify({"message": f"Execution method '{internal_execution_method}' not found"}), 404
        return jsonify(method.json()), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution method", "error": str(e)}), 500


@async_task_bp.route('/Update_ExecutionMethod/<string:internal_execution_method>', methods=['PUT'])
@jwt_required()
def Update_ExecutionMethod(internal_execution_method):
    try:
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        if execution_method:
            # Only update fields that are provided in the request
            if 'execution_method' in request.json:
                execution_method.execution_method = request.json.get('execution_method')
            if 'executor' in request.json:
                execution_method.executor = request.json.get('executor')
            if 'description' in request.json:
                execution_method.description = request.json.get('description')

            execution_method.last_updated_by = get_jwt_identity()

            # Update the last update timestamp
            execution_method.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)

        return make_response(jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error editing execution method", "error": str(e)}), 500)


@async_task_bp.route('/Delete_ExecutionMethod/<string:internal_execution_method>', methods=['DELETE'])
@jwt_required()
def Delete_ExecutionMethod(internal_execution_method):
    try:
        # Find the execution method by internal_execution_method
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        # If the execution method does not exist, return a 404 response
        if not execution_method:
            return jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404

        # Delete the execution method from the database
        db.session.delete(execution_method)
        db.session.commit()

        return jsonify({"message": f"Deleted successfully"}), 200

    except Exception as e:
        return jsonify({"error": "Failed to delete execution method", "details": str(e)}), 500

