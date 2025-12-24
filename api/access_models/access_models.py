from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_, func
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessModel,
    DefDataSource,
    DefAccessModelLogic
)
from . import access_models_bp

#def_access_models
@access_models_bp.route('/def_access_models', methods=['POST'])
@jwt_required()
def create_def_access_models():
    try:
        datasource_name = request.json.get('datasource_name', None)
        # Only validate foreign key if datasource_name is provided and not null/empty
        if datasource_name:
            datasource = DefDataSource.query.filter_by(datasource_name=datasource_name).first()
            if not datasource:
                return make_response(jsonify({"message": f"Datasource '{datasource_name}' does not exist"}), 400)

        new_def_access_model = DefAccessModel(
            model_name = request.json.get('model_name'),
            description = request.json.get('description'),
            type = request.json.get('type'),
            run_status = request.json.get('run_status'),
            state = request.json.get('state'),
            last_run_date = datetime.utcnow(),
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            datasource_name = datasource_name  # FK assignment
        )
        db.session.add(new_def_access_model)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@access_models_bp.route('/def_access_models', methods=['GET'])
@jwt_required()
def get_def_access_models():
    try:
        # Query parameters
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)
        model_name = request.args.get('model_name', '').strip()

        # Case 1: Get Single Model by ID
        if def_access_model_id:
            model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
            if model:
                return make_response(jsonify({"result": model.json()}), 200)
            else:
                return make_response(jsonify({"message": "Access Model not found"}), 404)

        # Base Query
        query = DefAccessModel.query

        # Case 2: Search by model_name
        if model_name:
            search_underscore = model_name.replace(' ', '_')
            search_space = model_name.replace('_', ' ')
            query = query.filter(
                or_(
                    DefAccessModel.model_name.ilike(f'%{model_name}%'),
                    DefAccessModel.model_name.ilike(f'%{search_underscore}%'),
                    DefAccessModel.model_name.ilike(f'%{search_space}%')
                )
            )

        # Order by ID descending
        query = query.order_by(DefAccessModel.def_access_model_id.desc())

        # Case 3: Pagination
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [model.json() for model in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (No pagination)
        models = query.all()
        return make_response(jsonify({
            "result": [model.json() for model in models]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving access models",
            "error": str(e)
        }), 500)



@access_models_bp.route('/def_access_models', methods=['PUT'])
@jwt_required()
def update_def_access_model():
    try:
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id query parameter is required'}), 400)
        model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
        if model:
            data = request.get_json()
            # Handle datasource_name FK update with case-insensitive and space-insensitive matching
            if 'datasource_name' in data:
                # Normalize input: strip, lower, remove underscores and spaces for matching
                input_ds = data['datasource_name'].strip().lower().replace('_', '').replace(' ', '')
                datasource = DefDataSource.query.filter(
                    func.replace(func.replace(func.lower(DefDataSource.datasource_name), '_', ''), ' ', '') == input_ds
                ).first()
                if not datasource:
                    return make_response(jsonify({"message": f"Datasource '{data['datasource_name']}' does not exist"}), 404)
                model.datasource_name = datasource.datasource_name  # Use the canonical name from DB

            model.model_name        = data.get('model_name', model.model_name)
            model.description       = data.get('description', model.description)
            model.type              = data.get('type', model.type)
            model.run_status        = data.get('run_status', model.run_status)
            model.state             = data.get('state', model.state)
            model.last_run_date     = datetime.utcnow()
            model.last_updated_by   = get_jwt_identity()
            model.last_update_date  = datetime.utcnow()
            model.revision          = model.revision + 1
            model.revision_date     = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error Editing Access Model', 'error': str(e)}), 500)

@access_models_bp.route('/def_access_models', methods=['DELETE'])
@jwt_required()
def delete_def_access_model():
    try:
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id query parameter is required'}), 400)
        model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
        if model:
            db.session.delete(model)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Access Model', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_models/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_access_model():
    try:
        # Get the access model ID from query params
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return jsonify({'error': 'def_access_model_id is required'}), 400


        access_model_exists = db.session.query(
            db.exists().where(DefAccessModel.def_access_model_id == def_access_model_id)
        ).scalar()

        access_model_logic_exists = db.session.query(
            db.exists().where(DefAccessModelLogic.def_access_model_id == def_access_model_id)
        ).scalar()

        if not access_model_exists and not access_model_logic_exists:
            return jsonify({'error': f'No records found in def_access_models or def_access_model_logics for ID {def_access_model_id}'}), 404

        DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).delete(synchronize_session=False)

        # Delete all related logic records
        DefAccessModelLogic.query.filter_by(def_access_model_id=def_access_model_id).delete(synchronize_session=False)

        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


