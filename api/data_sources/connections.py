from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from executors.extensions import db
from executors.models import DefDataSourceConnection
from utils.connectors import ConnectorManager

from . import data_sources_bp

@data_sources_bp.route('/test_connection', methods=['POST'])
@jwt_required()
def test_connection_from_payload():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"success": False, "message": "No JSON"}), 400)
        
        success, message = ConnectorManager.test(data)
        return make_response(jsonify({"success": success, "message": message}), 200 if success else 400)
    except Exception as e:
        return make_response(jsonify({"success": False, "message": str(e)}), 500)


@data_sources_bp.route('/def_data_source_connections', methods=['POST'])
@jwt_required()
def create_connection():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "No JSON"}), 400)
        
        conn_type = data.get('connection_type', '')
        password = data.get('password', '')
        
        connection = DefDataSourceConnection(
            def_data_source_id=data.get('def_data_source_id'),
            connection_type=conn_type,
            host=data.get('host'),
            port=data.get('port'),
            database_name=data.get('database_name'),
            username=data.get('username'),
            password=password,
            additional_params=data.get('additional_params', {}),
            is_active=data.get('is_active', True),
            created_by=get_jwt_identity(),
            last_updated_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        
        db.session.add(connection)
        db.session.commit()
        return make_response(jsonify({
            "message": "Added successfully", 
            "def_connection_id": connection.def_connection_id
        }), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)


@data_sources_bp.route('/def_data_source_connections', methods=['GET'])
@jwt_required()
def get_connections():
    try:
        conn_id = request.args.get('def_connection_id', type=int)
        if conn_id:
            conn = DefDataSourceConnection.query.get(conn_id)
            if not conn:
                return make_response(jsonify({"message": "Not found"}), 404)
            # Decrypt password for display? typically no, or mask it.
            # The .json() method doesn't return password usually.
            return make_response(jsonify({"result": conn.json()}), 200)

        # Return all connections (no pagination)
        connections = DefDataSourceConnection.query.order_by(DefDataSourceConnection.def_connection_id.desc()).all()
        
        return make_response(jsonify({
            "result": [c.json() for c in connections],
            "total": len(connections)
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": str(e)}), 500)


@data_sources_bp.route('/def_data_source_connections', methods=['PUT'])
@jwt_required()
def update_connection():
    try:
        conn_id = request.args.get('def_connection_id', type=int)
        if not conn_id:
            return make_response(jsonify({"message": "def_connection_id required"}), 400)

        conn = DefDataSourceConnection.query.get(conn_id)
        if not conn: return make_response(jsonify({"message": "Not found"}), 404)

        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "No JSON payload provided"}), 400)
        
        for field in ['host', 'port', 'database_name', 'username', 'connection_type', 'is_active', 'additional_params', 'def_data_source_id']:
            if field in data: setattr(conn, field, data[field])
        
        if 'password' in data and data['password']:
            conn.password = data['password']
        
        conn.last_updated_by = get_jwt_identity()
        conn.last_update_date = datetime.utcnow()
        
        db.session.commit()
        return make_response(jsonify({"message": "Updated"}), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)


@data_sources_bp.route('/def_data_source_connections', methods=['DELETE'])
@jwt_required()
def delete_connection():
    try:
        conn_id = request.args.get('def_connection_id', type=int)
        if not conn_id:
            return make_response(jsonify({"message": "def_connection_id required"}), 400)
            
        conn = DefDataSourceConnection.query.get(conn_id)
        if not conn: return make_response(jsonify({"message": "Not found"}), 404)
        
        db.session.delete(conn)
        db.session.commit()
        return make_response(jsonify({"message": "Deleted"}), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": str(e)}), 500)


@data_sources_bp.route('/def_data_source_connections/test/<int:def_connection_id>', methods=['POST'])
@jwt_required()
def test_saved_connection(def_connection_id):
    try:
        conn = DefDataSourceConnection.query.get(def_connection_id)
        if not conn: 
            return make_response(jsonify({"success": False, "message": "Not found"}), 404)

        config = conn.json()
        config['password'] = conn.password if conn.password else ''
        
        success, message = ConnectorManager.test(config) 
        return make_response(jsonify({"success": success, "message": message}), 200 if success else 400)
    except Exception as e:
        return make_response(jsonify({"success": False, "message": str(e)}), 500)

