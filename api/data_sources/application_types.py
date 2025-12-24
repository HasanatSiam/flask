from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from executors.extensions import db
from executors.models import DefDataSourceApplicationType
from . import data_sources_bp

@data_sources_bp.route('/def_application_types', methods=['POST'])
@jwt_required()
def create_application_type():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "No JSON payload provided"}), 400)
        
        # Check if type already exists with same version
        existing = DefDataSourceApplicationType.query.filter_by(
            application_type=data.get('application_type'),
            version=data.get('version')
        ).first()
        
        if existing:
            return make_response(jsonify({"message": "Application type with this version already exists"}), 400)
        
        app_type = DefDataSourceApplicationType(
            application_type=data.get('application_type'),
            version=data.get('version'),
            description=data.get('description'),
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_updated_by=get_jwt_identity(),
            last_update_date=datetime.utcnow()
        )
        
        db.session.add(app_type)
        db.session.commit()
        return make_response(jsonify({
            "message": "Added successfully", 
            "def_application_type_id": app_type.def_application_type_id
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)

@data_sources_bp.route('/def_application_types', methods=['GET'])
@jwt_required()
def get_application_types():
    try:
        app_type_id = request.args.get('def_application_type_id', type=int)
        if app_type_id:
            app_type = DefDataSourceApplicationType.query.get(app_type_id)
            return make_response(jsonify({"result": app_type.json()}), 200) if app_type else (jsonify({"message": "Not found"}), 404)

        # Optional filtering
        filters = []
        if request.args.get('application_type'):
            filters.append(DefDataSourceApplicationType.application_type.ilike(f"%{request.args.get('application_type')}%"))

        query = DefDataSourceApplicationType.query
        if filters:
            query = query.filter(*filters)
            
        results = query.order_by(DefDataSourceApplicationType.def_application_type_id.desc()).all()
        
        return make_response(jsonify({
            "result": [x.json() for x in results]
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": str(e)}), 500)

@data_sources_bp.route('/def_application_types', methods=['PUT'])
@jwt_required()
def update_application_type():
    try:
        app_type_id = request.args.get('def_application_type_id', type=int)
        if not app_type_id:
             return make_response(jsonify({"message": "def_application_type_id is required"}), 400)
             
        app_type = DefDataSourceApplicationType.query.get(app_type_id)
        if not app_type: 
            return make_response(jsonify({"message": "Not found"}), 404)

        data = request.get_json()
        
        if 'application_type' in data: app_type.application_type = data['application_type']
        if 'version' in data: app_type.version = data['version']
        if 'description' in data: app_type.description = data['description']
        
        app_type.last_updated_by = get_jwt_identity()
        app_type.last_update_date = datetime.utcnow()
        
        db.session.commit()
        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)

@data_sources_bp.route('/def_application_types', methods=['DELETE'])
@jwt_required()
def delete_application_type():
    try:
        app_type_id = request.args.get('def_application_type_id', type=int)
        if not app_type_id:
             return make_response(jsonify({"message": "def_application_type_id is required"}), 400)

        app_type = DefDataSourceApplicationType.query.get(app_type_id)
        if not app_type: 
            return make_response(jsonify({"message": "Not found"}), 404)
        
        db.session.delete(app_type)
        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)
