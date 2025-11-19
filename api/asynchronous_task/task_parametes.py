from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime


from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam

)
from . import async_task_bp


@async_task_bp.route('/Add_TaskParams/<string:task_name>', methods=['POST'])
@jwt_required()
def Add_TaskParams(task_name):
    try:
        # Check if the task exists in the DEF_ASYNC_TASKS table
        existing_task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not existing_task:
            return jsonify({"error": f"Task '{task_name}' does not exist"}), 404

        task_name = existing_task.task_name
        # Fetch parameters from the request
        parameters = request.json.get('parameters', [])
        if not parameters:
            return jsonify({"error": "No parameters provided"}), 400

        new_params = []
        for param in parameters:
            #seq = param.get('seq')
            parameter_name = param.get('parameter_name')
            data_type = param.get('data_type')
            description = param.get('description')
            

            # Validate required fields
            if not (parameter_name and data_type):
                return jsonify({"error": "Missing required parameter fields"}), 400

            # Create a new parameter object
            new_param = DefAsyncTaskParam(
                task_name = task_name,
                parameter_name = parameter_name,
                data_type = data_type,
                description = description,
                created_by = get_jwt_identity(),
                creation_date = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )
            new_params.append(new_param)

        # Add all new parameters to the session and commit
        db.session.add_all(new_params)
        db.session.commit()

        # return make_response(jsonify({
        #     "message": "Parameters Created successfully",
        #     "parameters": [param.json() for param in new_params]
        # }), 201)
        return make_response(jsonify({
            "message": "Added successfully",
        }), 201)
    except Exception as e:
        return jsonify({"error": "Failed to add task parameters", "details": str(e)}), 500



@async_task_bp.route('/Show_TaskParams/<string:task_name>', methods=['GET'])
@jwt_required()
def Show_Parameter(task_name):
    try:
        parameters = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()

        if not parameters:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify([param.json() for param in parameters]), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@async_task_bp.route('/Show_TaskParams/<string:task_name>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_TaskParams_Paginated(task_name, page, limit):
    try:
        query = DefAsyncTaskParam.query.filter_by(task_name=task_name)
        paginated = query.order_by(DefAsyncTaskParam.def_param_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify({
            "items": [param.json() for param in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@async_task_bp.route('/Update_TaskParams/<string:task_name>/<int:def_param_id>', methods=['PUT'])
@jwt_required()
def Update_TaskParams(task_name, def_param_id):
    try:
        # Get the updated values from the request body
        parameter_name = request.json.get('parameter_name')
        data_type = request.json.get('data_type')
        description = request.json.get('description')

        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Update the fields with the new values
        if parameter_name:
            param.parameter_name = parameter_name
        if data_type:
            param.data_type = data_type
        if description:
            param.description = description
        param.last_updated_by = get_jwt_identity()
        param.last_update_date = datetime.utcnow()

        # Commit the changes to the database
        db.session.commit()

        # return jsonify({"message": "Task parameter updated successfully", 
        #                  "task_param": param.json()}), 200
        return jsonify({"message": "Edited successfully"}), 200

    except Exception as e:
        return jsonify({"error": "Error editing task parameter", "details": str(e)}), 500



@async_task_bp.route('/Delete_TaskParams/<string:task_name>/<int:def_param_id>', methods=['DELETE'])
@jwt_required()
def Delete_TaskParams(task_name, def_param_id):
    try:
        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Delete the parameter from the database
        db.session.delete(param)
        db.session.commit()

        # return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' successfully deleted from task '{task_name}'"}), 200
        return jsonify({"message": "Deleted successfully"}), 200


    except Exception as e:
        return jsonify({"error": "Failed to delete task parameter", "details": str(e)}), 500


