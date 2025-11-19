from sqlalchemy import or_
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (

    DefGlobalCondition,
    DefGlobalConditionLogic
)

from . import glob_conditions_bp

# def_global_conditions
@glob_conditions_bp.route('/def_global_conditions', methods=['POST'])
@jwt_required()
def create_def_global_condition():
    try:
        name        = request.json.get('name')
        datasource  = request.json.get('datasource')
        description = request.json.get('description')
        status      = request.json.get('status')

        new_condition = DefGlobalCondition(
            name        = name,
            datasource  = datasource,
            description = description,
            status      = status,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_condition)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@glob_conditions_bp.route('/def_global_conditions', methods=['GET'])
@jwt_required()
def get_def_global_conditions():
    try:
        conditions = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc()).all()
        return make_response(jsonify([condition.json() for condition in conditions]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving GlobalConditions", "error": str(e)}), 500)


@glob_conditions_bp.route('/def_global_conditions/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_global_conditions(page, limit):
    try:
        search_query = request.args.get('name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefGlobalCondition.query

        if search_query:
            query = query.filter(
                or_(
                    DefGlobalCondition.name.ilike(f'%{search_query}%'),
                    DefGlobalCondition.name.ilike(f'%{search_underscore}%'),
                    DefGlobalCondition.name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefGlobalCondition.def_global_condition_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({
            "message": "Error searching Global Conditions",
            "error": str(e)
        }), 500)


@glob_conditions_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['GET'])
@jwt_required()
def get_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            return make_response(jsonify(condition.json()), 200)
        return make_response(jsonify({"message": "Global condition not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Global Condition", "error": str(e)}), 500)


@glob_conditions_bp.route('/def_global_conditions/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_def_global_conditions(page, limit):
    try:
        query = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving Global Conditions",
            "error": str(e)
        }), 500)


@glob_conditions_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['PUT'])
@jwt_required()
def update_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            condition.name        = request.json.get('name', condition.name)
            condition.datasource  = request.json.get('datasource', condition.datasource)
            condition.description = request.json.get('description', condition.description)
            condition.status      = request.json.get('status', condition.status)
            condition.last_updated_by = get_jwt_identity()
            condition.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Global Condition', 'error': str(e)}), 500)

@glob_conditions_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            db.session.delete(condition)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Global Condition', 'error': str(e)}), 500)



@glob_conditions_bp.route('/def_global_conditions/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_global_condition():
    try:
        # Get condition ID from query parameter
        def_global_condition_id = request.args.get('def_global_condition_id', type=int)
        if not def_global_condition_id:
            return jsonify({'error': 'def_global_condition_id is required'}), 400
        
        global_condition_exists = db.session.query(
            db.exists().where(DefGlobalCondition.def_global_condition_id == def_global_condition_id)
        ).scalar()

        global_condition_logic_exists = db.session.query(
            db.exists().where(DefGlobalConditionLogic.def_global_condition_id == def_global_condition_id)
        ).scalar()

        if not global_condition_exists and not global_condition_logic_exists:
            return jsonify({'error': f'No records found in def_global_conditions or def_global_condition_logics for ID {def_global_condition_id}'}), 404

        DefGlobalConditionLogic.query.filter_by(def_global_condition_id=def_global_condition_id).delete(synchronize_session=False)

        DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).delete(synchronize_session=False)

        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

