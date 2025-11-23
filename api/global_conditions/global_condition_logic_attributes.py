from sqlalchemy.exc import IntegrityError
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses
from flask_jwt_extended import jwt_required, get_jwt_identity
from executors.extensions import db
from executors.models import (
    DefGlobalConditionLogic,
    DefGlobalConditionLogicAttribute
)

from . import global_conditions_bp


# def_global_condition_logics_attributes
@global_conditions_bp.route('/def_global_condition_logic_attributes', methods=['POST'])
@jwt_required()
def create_def_global_condition_logic_attribute():
    try:
        id = request.json.get('id')
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not all([id, def_global_condition_logic_id]):
            return make_response(jsonify({'message': 'Both id and def_global_condition_logic_id are required'}), 400)

        # Check if the ID already exists
        existing = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
        if existing:
            return make_response(jsonify({'message': f'Attribute with id {id} already exists'}), 409)

        # Check if foreign key exists
        logic_exists = db.session.query(
            db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
        ).scalar()

        if not logic_exists:
            return make_response(jsonify({
                'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
            }), 404)

        # Create new record
        new_attr = DefGlobalConditionLogicAttribute(
            id = id,
            def_global_condition_logic_id = def_global_condition_logic_id,
            widget_position = widget_position,
            widget_state = widget_state,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_attr)
        db.session.commit()

        # return make_response(jsonify({
        #     'id': new_attr.id,
        #     'message': 'Attribute created successfully'
        # }), 201)
        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error (possibly duplicate key)'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating attribute', 'error': str(e)}), 500)



@global_conditions_bp.route('/def_global_condition_logic_attributes', methods=['GET'])
@jwt_required()
def get_def_global_condition_logic_attributes():
    try:
        id = request.args.get('id', type=int)

        # Case 1: Get single attribute by ID
        if id:
            attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
            if attribute:
                return make_response(jsonify({"result": attribute.json()}), 200)
            return make_response(jsonify({
                "message": "Global Condition Logic Attribute not found"
            }), 404)

        # Case 2: Get all attributes
        attributes = DefGlobalConditionLogicAttribute.query.order_by(
            DefGlobalConditionLogicAttribute.id.desc()
        ).all()

        return make_response(jsonify({
            "result": [attribute.json() for attribute in attributes]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving condition logic attributes",
            "error": str(e)
        }), 500)
   



@global_conditions_bp.route('/def_global_condition_logic_attributes/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_def_global_condition_logic_attributes(page, limit):
    try:
        query = DefGlobalConditionLogicAttribute.query.order_by(DefGlobalConditionLogicAttribute.id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching global condition logic attributes',
            'error': str(e)
        }), 500)


@global_conditions_bp.route('/def_global_condition_logic_attributes/upsert', methods=['POST'])
@jwt_required()
def upsert_def_global_condition_logic_attributes():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

        for data in data_list:
            id = data.get('id')
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attr = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

            if existing_attr:
                # Prevent changing foreign key
                if def_global_condition_logic_id and def_global_condition_logic_id != existing_attr.def_global_condition_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_logic_id is not allowed'
                    })
                    continue

                existing_attr.widget_position = widget_position
                existing_attr.widget_state = widget_state
                existing_attr.last_updated_by = get_jwt_identity()
                existing_attr.last_update_date = datetime.utcnow()

                db.session.add(existing_attr)

                # response.append({
                #     'id': existing_attr.id,
                #     'status': 'updated',
                #     'message': 'Attribute updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

            else:
                # Validate required FK
                if not def_global_condition_logic_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_logic_id is required for new records'
                    })
                    continue

                # Check foreign key existence
                logic_exists = db.session.query(
                    db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
                ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
                    })
                    continue

                if not id:
                    response.append({
                        'status': 'error',
                        'message': 'id is required for new records'
                    })
                    continue

                new_attr = DefGlobalConditionLogicAttribute(
                    id=id,
                    def_global_condition_logic_id = def_global_condition_logic_id,
                    widget_position = widget_position,
                    widget_state = widget_state,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_attr)
                db.session.flush()

                # response.append({
                #     'id': new_attr.id,
                #     'status': 'created',
                #     'message': 'Attribute created successfully'
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


@global_conditions_bp.route('/def_global_condition_logic_attributes', methods=['PUT'])
@jwt_required()
def update_def_global_condition_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        data = request.get_json()
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'Global Condition Logic Attribute not found'}), 404)

        # Update allowed fields
        attribute.widget_position = data.get('widget_position', attribute.widget_position)
        attribute.widget_state = data.get('widget_state', attribute.widget_state)
        attribute.last_updated_by = get_jwt_identity()
        attribute.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error editing Global Condition Logic Attribute',
            'error': str(e)
        }), 500)




@global_conditions_bp.route('/def_global_condition_logic_attributes', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'Global Condition Logic Attribute not found'}), 404)

        db.session.delete(attribute)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting Global Condition Logic Attribute',
            'error': str(e)
        }), 500)








