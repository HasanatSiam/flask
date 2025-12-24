from sqlalchemy.exc import IntegrityError
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity
from executors.extensions import db

from executors.models import (
    DefGlobalCondition,
    DefGlobalConditionLogic
)

from . import global_conditions_bp


# def_global_condition_logics
@global_conditions_bp.route('/def_global_condition_logics', methods=['POST'])
@jwt_required()
def create_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')

        if def_global_condition_logic_id is None:
            return make_response(jsonify({"message": "Missing 'def_global_condition_logic_id'"}), 400)

        # Check if ID already exists
        existing = db.session.get(DefGlobalConditionLogic, def_global_condition_logic_id)
        if existing:
            return make_response(jsonify({
                "message": f"Global Condition Logic ID {def_global_condition_logic_id} already exists."
            }), 409)
        
        def_global_condition_id = request.json.get('def_global_condition_id')
        object = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

    
        new_logic = DefGlobalConditionLogic(
            def_global_condition_logic_id = def_global_condition_logic_id,
            def_global_condition_id = def_global_condition_id,
            object = object,
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
        # return make_response(jsonify({'def_global_condition_logic_id' : new_logic.def_global_condition_logic_id,
        #                               'message': 'Global Condition Logic created successfully'}), 201)
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@global_conditions_bp.route('/def_global_condition_logics/upsert', methods=['POST'])
@jwt_required()
def upsert_def_global_condition_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

        for data in data_list:
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            def_global_condition_id = data.get('def_global_condition_id')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition = data.get('condition')
            value = data.get('value')

            existing_logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()

            if existing_logic:
                # Prevent changing foreign key
                if def_global_condition_id and def_global_condition_id != existing_logic.def_global_condition_id:
                    response.append({
                        'def_global_condition_logic_id': def_global_condition_logic_id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_id is not allowed'
                    })
                    continue

                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                existing_logic.last_updated_by = get_jwt_identity()
                existing_logic.last_update_date = datetime.utcnow()


                db.session.add(existing_logic)

                # response.append({
                #     'def_global_condition_logic_id': existing_logic.def_global_condition_logic_id,
                #     'status': 'updated',
                #     'message': 'Logic updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

            else:
                if not def_global_condition_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                condition_exists = db.session.query(
                    db.exists().where(DefGlobalCondition.def_global_condition_id == def_global_condition_id)
                ).scalar()

                if not condition_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_id {def_global_condition_id} does not exist'
                    })
                    continue

                new_logic = DefGlobalConditionLogic(
                    def_global_condition_logic_id = def_global_condition_logic_id,
                    def_global_condition_id = def_global_condition_id,
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
                #     'def_global_condition_logic_id': new_logic.def_global_condition_logic_id,
                #     'status': 'created',
                #     'message': 'Logic created successfully'
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


@global_conditions_bp.route('/def_global_condition_logics', methods=['GET'])
@jwt_required()
def get_def_global_condition_logics():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)

        # Case 1: Single logic by ID
        if def_global_condition_logic_id:
            logic = DefGlobalConditionLogic.query.filter_by(
                def_global_condition_logic_id=def_global_condition_logic_id
            ).first()

            if logic:
                return make_response(jsonify({"result": logic.json()}), 200)

            return make_response(jsonify({
                "message": "Global Condition Logic not found"
            }), 404)

        # Case 2: Get all logics
        logics = DefGlobalConditionLogic.query.order_by(
            DefGlobalConditionLogic.def_global_condition_logic_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [logic.json() for logic in logics]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving Global Condition Logics",
            "error": str(e)
        }), 500)



@global_conditions_bp.route('/def_global_condition_logics', methods=['PUT'])
@jwt_required()
def update_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)
        if not def_global_condition_logic_id:
            return make_response(jsonify({'message': 'def_global_condition_logic_id query parameter is required'}), 400)
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            logic.def_global_condition_id = request.json.get('def_global_condition_id', logic.def_global_condition_id)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)
            logic.last_updated_by = get_jwt_identity()
            logic.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Global Condition Logic', 'error': str(e)}), 500)


@global_conditions_bp.route('/def_global_condition_logics', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)
        if not def_global_condition_logic_id:
            return make_response(jsonify({'message': 'def_global_condition_logic_id query parameter is required'}), 400)
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Global Condition Logic', 'error': str(e)}), 500)


