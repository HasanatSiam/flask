from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAsyncTask

)
from . import async_task_bp


# Create a task definition
@async_task_bp.route('/Create_Task', methods=['POST'])
@jwt_required()
def Create_Task():
    try:
        user_task_name = request.json.get('user_task_name')
        task_name = request.json.get('task_name')
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        script_name = request.json.get('script_name')
        script_path = request.json.get('script_path')
        description = request.json.get('description')
        srs = request.json.get('srs')
        sf  = request.json.get('sf')

        new_task = DefAsyncTask(
            user_task_name = user_task_name,
            task_name = task_name,
            execution_method = execution_method,
            internal_execution_method = internal_execution_method,
            executor = executor,
            script_name = script_name,
            script_path = script_path,
            description = description,
            cancelled_yn = 'N',
            srs = srs,
            sf  = sf,
            created_by = get_jwt_identity(),
            last_updated_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_update_date = datetime.utcnow()

        )
        db.session.add(new_task)
        db.session.commit()

        return {"message": "Added successfully"}, 201

    except Exception as e:
        return {"message": "Error creating Task", "error": str(e)}, 500


@async_task_bp.route('/def_async_tasks', methods=['GET'])
@jwt_required()
def Show_Tasks():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


@async_task_bp.route('/def_async_tasks/v1', methods=['GET'])
def Show_Tasks_v1():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


@async_task_bp.route('/def_async_tasks/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_Tasks_Paginated(page, limit):
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.creation_date.desc())
        paginated = tasks.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


@async_task_bp.route('/def_async_tasks/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_tasks_show_tasks(page, limit):
    try:
        search_query = request.args.get('user_task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTask.query
        if search_query:
            
            query = query.filter(or_(
                DefAsyncTask.user_task_name.ilike(f'%{search_query}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_space}%')
            ))
        paginated = query.order_by(DefAsyncTask.def_task_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching tasks", "error": str(e)}), 500)



@async_task_bp.route('/Show_Task/<task_name>', methods=['GET'])
@jwt_required()
def Show_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not task:
            return {"message": f"Task with name '{task_name}' not found"}, 404

        return make_response(jsonify(task.json()), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting the task", "error": str(e)}), 500)


@async_task_bp.route('/Update_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Update_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if task:
            # Only update fields that are provided in the request
            if 'user_task_name' in request.json:
                task.user_task_name = request.json.get('user_task_name')
            if 'execution_method' in request.json:
                task.execution_method = request.json.get('execution_method')
            if 'script_name' in request.json:
                task.script_name = request.json.get('script_name')
            if 'description' in request.json:
                task.description = request.json.get('description')
            if 'srs' in request.json:
                task.srs = request.json.get('srs')
            if 'sf' in request.json:
                task.sf = request.json.get('sf')
            task.last_updated_by = get_jwt_identity()
            task.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)

        return make_response(jsonify({"message": f"Async Task with name '{task_name}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error editing async Task", "error": str(e)}), 500)


@async_task_bp.route('/Cancel_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Cancel_Task(task_name):
    try:
        # Find the task by task_name in the DEF_ASYNC_TASKS table
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if task:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            task.cancelled_yn = 'Y'

            db.session.commit()

            # return make_response(jsonify({"message": f"Task {task_name} has been cancelled successfully"}), 200)
            return make_response(jsonify({"message": "Cancelled successfully"}), 200)


        return make_response(jsonify({"message": f"Task {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error cancelling Task", "error": str(e)}), 500)



