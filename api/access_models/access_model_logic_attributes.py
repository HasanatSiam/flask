from . import access_models_bp
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessModelLogic,
    DefAccessModelLogicAttribute
)



#def_access_model_logic_attributes
@access_models_bp.route('/def_access_model_logic_attributes', methods=['POST'])
@jwt_required()
def create_def_access_model_logic_attribute():
    try:
        id = request.json.get('id')
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id is required'}), 400)
        if DefAccessModelLogicAttribute.query.filter_by(id=id).first():
            return make_response(jsonify({'message': f'id {id} already exists'}), 409)
        # Check if def_access_model_logic_id exists in DefAccessModelLogic table
        logic_id_exists = db.session.query(
            db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
        ).scalar()
        if not logic_id_exists:
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'}), 400)
        
        
        new_attribute = DefAccessModelLogicAttribute(
            id = id,
            def_access_model_logic_id = def_access_model_logic_id,
            widget_position = widget_position,
            widget_state = widget_state,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_attribute)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@access_models_bp.route('/def_access_model_logic_attributes', methods=['GET'])
@jwt_required()
def get_def_access_model_logic_attributes():
    try:
        id = request.args.get('id', type=int)

        # Case 1: Get single attribute by ID
        if id:
            attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
            if attribute:
                return make_response(jsonify({"result": attribute.json()}), 200)
            else:
                return make_response(jsonify({'message': 'Attribute not found'}), 404)

        # Case 2: Get all attributes
        attributes = DefAccessModelLogicAttribute.query.order_by(
            DefAccessModelLogicAttribute.id.desc()
        ).all()

        return make_response(jsonify({
            "result": [attr.json() for attr in attributes]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving attributes",
            "error": str(e)
        }), 500)


@access_models_bp.route('/def_access_model_logic_attributes/upsert', methods=['POST'])
@jwt_required()
def upsert_def_access_model_logic_attributes():
    try:
        data_list = request.get_json()

        # Enforce list-only payload
        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

        for data in data_list:
            id = data.get('id')
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
            if existing_attribute:
                # if not attribute:
                #     response.append({
                #         'id': attribute_id,
                #         'status': 'error',
                #         'message': f'Attribute with id {attribute_id} not found'
                #     })
                #     continue

                # Disallow updating def_access_model_logic_id
                if def_access_model_logic_id and def_access_model_logic_id != existing_attribute.def_access_model_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_access_model_logic_id is not allowed'
                    })
                    continue

                existing_attribute.widget_position = widget_position
                existing_attribute.widget_state = widget_state
                existing_attribute.last_updated_by = get_jwt_identity()
                existing_attribute.last_update_date = datetime.utcnow()

                db.session.add(existing_attribute)

                # response.append({
                #     'id': existing_attribute.id,
                #     'status': 'updated',
                #     'message': 'Attribute updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

            else:
                # Take the maximum data of foreign-key from foreign table
                # def_access_model_logic_id = db.session.query(
                #     func.max(DefAccessModelLogic.def_access_model_logic_id)
                # ).scalar()

                # if def_access_model_logic_id is None:
                #     response.append({
                #         'status': 'error',
                #         'message': 'No DefAccessModelLogic entries exist to assign logic ID'
                #     })
                #     continue

                # Validate def_access_model_logic_id exists
                logic_exists = db.session.query(
                    db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
                    ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'
                    })
                    continue

                new_attribute = DefAccessModelLogicAttribute(
                    id = id,
                    def_access_model_logic_id = def_access_model_logic_id,
                    widget_position = widget_position,
                    widget_state = widget_state,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_attribute)
                db.session.flush()

                # response.append({
                #     'id': new_attribute.id,
                #     'status': 'created',
                #     'message': 'Attribute created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created =True


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


@access_models_bp.route('/def_access_model_logic_attributes', methods=['PUT'])
@jwt_required()
def update_def_access_model_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            # attribute.def_access_model_logic_id = request.json.get('def_access_model_logic_id', attribute.def_access_model_logic_id)
            attribute.widget_position  = request.json.get('widget_position', attribute.widget_position)
            attribute.widget_state     = request.json.get('widget_state', attribute.widget_state)
            attribute.last_updated_by  = get_jwt_identity()
            attribute.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing attribute', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logic_attributes', methods=['DELETE'])
@jwt_required()
def delete_def_access_model_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            db.session.delete(attribute)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting attribute', 'error': str(e)}), 500)

