from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefControl
)

from . import controls_bp






#DEF_CONTROLS
@controls_bp.route('/def_controls', methods=['GET'])
@jwt_required()
def get_def_controls():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        control_name = request.args.get('control_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_control_id:
            control = DefControl.query.filter_by(def_control_id=def_control_id).first()
            if control:
                return make_response(jsonify({'result': control.json()}), 200)
            return make_response(jsonify({'message': 'Control not found'}), 404)

        query = DefControl.query

        # Case 2: Search
        if control_name:
            search_query = control_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefControl.control_name.ilike(f'%{search_query}%'),
                    DefControl.control_name.ilike(f'%{search_underscore}%'),
                    DefControl.control_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [control.json() for control in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        controls = query.order_by(DefControl.def_control_id.desc()).all()
        return make_response(jsonify({'result': [c.json() for c in controls]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching controls', 'error': str(e)}), 500)


@controls_bp.route('/def_controls', methods=['POST'])
@jwt_required()
def create_control():
    try:
        new_control = DefControl(
            control_name = request.json.get('control_name'),
            description = request.json.get('description'),
            pending_results_count = request.json.get('pending_results_count'),
            control_type = request.json.get('control_type'),
            priority = request.json.get('priority'),
            datasources = request.json.get('datasources'),
            last_run_date = datetime.utcnow(),
            status = request.json.get('status'),
            state = request.json.get('state'),
            result_investigator = request.json.get('result_investigator'),
            authorized_data = request.json.get('authorized_data'),
            revision = 0,
            revision_date = datetime.utcnow(),
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_control)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error adding control', 'error': str(e)}), 500)



@controls_bp.route('/def_controls', methods=['PUT'])
@jwt_required()
def update_control():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        if not def_control_id:
            return make_response(jsonify({'message': 'def_control_id is required'}), 400)

        control = DefControl.query.filter_by(def_control_id=def_control_id).first()
        if control:
            control.control_name = request.json.get('control_name', control.control_name)
            control.description = request.json.get('description', control.description)
            control.pending_results_count = request.json.get('pending_results_count', control.pending_results_count)
            control.control_type = request.json.get('control_type', control.control_type)
            control.priority = request.json.get('priority', control.priority)
            control.datasources = request.json.get('datasources', control.datasources)
            control.last_run_date = datetime.utcnow()
            control.status = request.json.get('status', control.status)
            control.state = request.json.get('state', control.state)
            control.result_investigator = request.json.get('result_investigator', control.result_investigator)
            control.authorized_data = request.json.get('authorized_data', control.authorized_data)
            control.revision += 1
            control.revision_date = datetime.utcnow()
            control.created_by = get_jwt_identity()
            control.creation_date = datetime.utcnow()
            control.last_updated_by = get_jwt_identity()
            control.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing control', 'error': str(e)}), 500)


@controls_bp.route('/def_controls', methods=['DELETE'])
@jwt_required()
def delete_control():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        if not def_control_id:
            return make_response(jsonify({'message': 'def_control_id is required'}), 400)

        control = DefControl.query.filter_by(def_control_id=def_control_id).first()
        if control:
            db.session.delete(control)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting control', 'error': str(e)}), 500)

