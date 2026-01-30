from flask import request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from executors.extensions import db
from executors.models import DefProcessNodeType
from . import workflow_bp

@workflow_bp.route('/workflow/node_types', methods=['GET'])
@jwt_required()
def get_node_types():
    try:
        def_node_type_id = request.args.get('def_node_type_id')
        if def_node_type_id:
            node_type = DefProcessNodeType.query.get(def_node_type_id)
            if not node_type:
                return jsonify({"error": "Node type not found"}), 404
            return jsonify({"result": node_type.json()}), 200
            
        node_types = DefProcessNodeType.query.all()
        return jsonify({"result": [n.json() for n in node_types]}), 200
    except Exception as e:
        return jsonify({"message": "Error getting node types", "error": str(e)}), 500


@workflow_bp.route('/workflow/node_types', methods=['POST'])
@jwt_required()
def create_node_type():
    try:
        data = request.json
        shape_name = data.get('shape_name')
        behavior = data.get('behavior')
        
        if not shape_name or not behavior:
            return jsonify({"error": "shape_name and behavior are required"}), 400
            
        existing = DefProcessNodeType.query.filter_by(shape_name=shape_name).first()
        if existing:
            return jsonify({"error": f"Node type with shape_name '{shape_name}' already exists"}), 409
            
        new_type = DefProcessNodeType(
            shape_name=shape_name,
            behavior=behavior,
            display_name=data.get('display_name'),
            requires_step_function=data.get('requires_step_function', 'N'),
            description=data.get('description'),
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_updated_by=get_jwt_identity(),
            last_update_date=datetime.utcnow()
        )
        
        db.session.add(new_type)
        db.session.commit()
        
        return jsonify({"message": "Added successfully", "result": new_type.json()}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error creating node type", "error": str(e)}), 500


@workflow_bp.route('/workflow/node_types', methods=['PUT', 'DELETE'])
@jwt_required()
def manage_node_type():
    try:
        def_node_type_id = request.args.get('def_node_type_id')
        if not def_node_type_id:
            return jsonify({"error": "Missing def_node_type_id query parameter"}), 400
            
        node_type = DefProcessNodeType.query.get(def_node_type_id)
        if not node_type:
            return jsonify({"error": "Node type not found"}), 404
            
        if request.method == 'DELETE':
            db.session.delete(node_type)
            db.session.commit()
            return jsonify({"message": "Node type deleted"}), 200
            
        # PUT method
        data = request.json
        if 'shape_name' in data:
             # Ensure uniqueness if changing shape_name
             new_shape = data['shape_name']
             if new_shape != node_type.shape_name:
                 existing = DefProcessNodeType.query.filter_by(shape_name=new_shape).first()
                 if existing:
                     return jsonify({"error": f"Shape name '{new_shape}' already exists"}), 409
                 node_type.shape_name = new_shape
                 
        if 'behavior' in data: node_type.behavior = data['behavior']
        if 'display_name' in data: node_type.display_name = data['display_name']
        if 'requires_step_function' in data: node_type.requires_step_function = data['requires_step_function']
        if 'description' in data: node_type.description = data['description']
        
        node_type.last_updated_by = get_jwt_identity()
        node_type.last_update_date = datetime.utcnow()
        
        db.session.commit()
        return jsonify({"message": "Edited successfully", "result": node_type.json()}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": f"Error during {request.method} node type", "error": str(e)}), 500
