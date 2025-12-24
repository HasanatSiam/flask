from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessModelLogic,
    DefAccessModel
)
from . import access_models_bp




#def_access_model_logics
@access_models_bp.route('/def_access_model_logics', methods=['POST'])
@jwt_required()
def create_def_access_model_logic():
    try:
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        def_access_model_id = request.json.get('def_access_model_id')
        filter_text = request.json.get('filter')
        object_text = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id is required'}), 400)
        
        if DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first():
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} already exists'}), 409)

        # Check if def_access_model_id exists in DefAccessModel table
        model_id_exists = db.session.query(
            db.exists().where(DefAccessModel.def_access_model_id == def_access_model_id)
        ).scalar()
        if not model_id_exists:
            return make_response(jsonify({'message': f'def_access_model_id {def_access_model_id} does not exist'}), 400)

        new_logic = DefAccessModelLogic(
            def_access_model_logic_id = def_access_model_logic_id,
            def_access_model_id = def_access_model_id,
            filter = filter_text,
            object = object_text,
            attribute = attribute,
            condition = condition,
            value = value,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': f'Error: {str(e)}'}), 500)

@access_models_bp.route('/def_access_model_logics/upsert', methods=['POST'])
@jwt_required()
def upsert_def_access_model_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a non-empty list of objects'}), 400)

        response = []
        created = False

        for data in data_list:
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            model_id = data.get('def_access_model_id')
            filter_text = data.get('filter')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition  = data.get('condition')
            value = data.get('value')

            existing_logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()

            if existing_logic:
                # if not logic:
                #     response.append({
                #         'def_access_model_logic_id': logic_id,
                #         'status': 'error',
                #         'message': f'DefAccessModelLogic with id {logic_id} not found'
                #     })
                #     continue

                # Prevent changing foreign key
                if model_id and model_id != existing_logic.def_access_model_id:
                    response.append({
                        'def_access_model_logic_id': def_access_model_logic_id,
                        'status': 'error',
                        'message': 'Updating def_access_model_id is not allowed'
                    })
                    continue

               
                existing_logic.filter = filter_text
                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                existing_logic.last_updated_by = get_jwt_identity()
                existing_logic.last_update_date = datetime.utcnow()


                db.session.add(existing_logic)

                # response.append({
                #     'def_access_model_logic_id': existing_logic.def_access_model_logic_id,
                #     'status': 'updated',
                #     'message': 'AccessModelLogic updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

            else:
                if not model_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_access_model_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                model_exists = db.session.query(
                    db.exists().where(DefAccessModel.def_access_model_id == model_id)
                ).scalar()

                if not model_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_id {model_id} does not exist'
                    })
                    continue

                new_logic = DefAccessModelLogic(
                    def_access_model_logic_id = def_access_model_logic_id,
                    def_access_model_id = model_id,
                    filter = filter_text,
                    object = object_text,
                    attribute = attribute,
                    condition = condition,
                    value = value,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_logic)
                db.session.flush()

                # response.append({
                #     'def_access_model_logic_id': new_logic.def_access_model_logic_id,
                #     'status': 'created',
                #     'message': 'AccessModelLogic created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created = True

        db.session.commit()

        status_code = 201 if created else 200
        return make_response(jsonify(response), status_code)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)



@access_models_bp.route('/def_access_model_logics', methods=['GET'])
@jwt_required()
def get_def_access_model_logics():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)

        # Case 1: Get single record
        if def_access_model_logic_id:
            logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
            if logic:
                return make_response(jsonify({"result": logic.json()}), 200)
            else:
                return make_response(jsonify({'message': 'access model logic not found'}), 404)

        # Case 2: Get all
        logics = DefAccessModelLogic.query.order_by(
            DefAccessModelLogic.def_access_model_logic_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [logic.json() for logic in logics]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error retrieving access model logics',
            'error': str(e)
        }), 500)




@access_models_bp.route('/def_access_model_logics', methods=['PUT'])
@jwt_required()
def update_def_access_model_logic():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)
        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id query parameter is required'}), 400)
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
        if logic:
            # logic.def_access_model_id = request.json.get('def_access_model_id', logic.def_access_model_id)
            logic.filter = request.json.get('filter', logic.filter)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)
            logic.last_updated_by = get_jwt_identity()
            logic.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Access Model Logic', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logics', methods=['DELETE'])
@jwt_required()
def delete_def_access_model_logic():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)
        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id query parameter is required'}), 400)
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Access Model Logic', 'error': str(e)}), 500)


