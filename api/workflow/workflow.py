"""
Workflow API Endpoints

CRUD for workflows.
"""

from flask import request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from executors.extensions import db
from executors.models import DefProcess
from workflow_engine import WorkflowEngine, WorkflowError

from . import workflow_bp


@workflow_bp.route('/workflow', methods=['POST'])
@jwt_required()
def create_workflow():
    try:
        data = request.json
        process_name = data.get('process_name')
        process_structure = data.get('process_structure')
        
        if not process_name:
            return jsonify({"error": "process_name is required"}), 400
        
        # Check for duplicate name if needed (optional)
        existing = DefProcess.query.filter_by(process_name=process_name).first()
        if existing:
             return jsonify({"error": "Workflow name already exists"}), 409
        
        workflow = DefProcess(
            process_name=process_name,
            process_structure=process_structure,
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_updated_by=get_jwt_identity(),
            last_update_date=datetime.utcnow()
        )
        
        db.session.add(workflow)
        db.session.commit()
        
        return jsonify({"message": "Added successfully", "result": workflow.json()}), 201
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error creating workflow", "error": str(e)}), 500


@workflow_bp.route('/workflow', methods=['PUT'])
@jwt_required()
def update_workflow():
    try:
        process_id = request.args.get('process_id')
        if not process_id:
             return jsonify({"error": "Missing process_id query parameter"}), 400
             
        workflow = DefProcess.query.get(process_id)
        if not workflow:
            return jsonify({"error": "Workflow not found"}), 404
        
        data = request.get_json(silent=True) or request.json
        
        if 'process_name' in data:
            workflow.process_name = data['process_name']
        
        if 'process_structure' in data:
            workflow.process_structure = data['process_structure']
        
        workflow.last_updated_by = get_jwt_identity()
        workflow.last_update_date = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({"message": "Edited successfully", "result": workflow.json()}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error updating workflow", "error": str(e)}), 500


@workflow_bp.route('/workflow', methods=['GET'])
@jwt_required()
def get_all_workflows():
    try:
        process_id = request.args.get('process_id')
        process_name = request.args.get('process_name')
        
        query = DefProcess.query
        
        if process_id:
            query = query.filter_by(process_id=process_id)
        if process_name:
            query = query.filter_by(process_name=process_name)
            
        workflows = query.order_by(DefProcess.creation_date.desc()).all()
        return jsonify({"result": [w.json() for w in workflows]}), 200
    except Exception as e:
        return jsonify({"message": "Error fetching workflows", "error": str(e)}), 500


@workflow_bp.route('/workflow', methods=['DELETE'])
@jwt_required()
def delete_workflow():
    try:
        process_id = request.args.get('process_id')
        process_name = request.args.get('process_name')
        
        workflow = None
        if process_id:
            workflow = DefProcess.query.get(process_id)
        elif process_name:
            workflow = DefProcess.query.filter_by(process_name=process_name).first()
            
        if not workflow:
            return jsonify({"error": "Workflow not found or missing identifiers"}), 404
        
        db.session.delete(workflow)
        db.session.commit()
        
        return jsonify({"message": "Workflow deleted"}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error deleting workflow", "error": str(e)}), 500


@workflow_bp.route('/workflow/validate', methods=['POST'])
@jwt_required()
def validate_workflow():
    try:
        data = request.get_json(silent=True)
        if not data or 'process_structure' not in data:
             return jsonify({"error": "process_structure is required"}), 400
             
        engine = WorkflowEngine()
        errors = engine.validate(data['process_structure'])
        
        if errors:
            return jsonify({"valid": False, "errors": errors}), 200
        
        return jsonify({"valid": True, "errors": []}), 200
        
    except Exception as e:
        return jsonify({"message": "Error validating workflow", "error": str(e)}), 500


@workflow_bp.route('/workflow/run/<int:process_id>', methods=['POST'])
@jwt_required()
def run_workflow(process_id):
    """
    Run a workflow asynchronously.
    Returns the execution_id immediately.
    """
    try:
        from threading import Thread
        from flask import current_app
        
        context = {}
        data = request.get_json(silent=True)
        if data:
            context = data.get('context', {})
        
        engine = WorkflowEngine()
        user_id = get_jwt_identity()
        
        # 1. Initialize the execution record (Sync)
        def_process_execution_id = engine.initialize_execution(process_id, context, user_id)
        
        # 2. Run the engine (Async)
        # We use a closure to ensure app context is available in the thread
        app = current_app._get_current_object()
        def background_run():
            with app.app_context():
                try:
                    engine.execute_from_id(def_process_execution_id)
                except Exception as e:
                    print(f"Background execution failed for {def_process_execution_id}: {e}")

        Thread(target=background_run).start()
        
        return jsonify({
            "message": "Workflow started",
            "def_process_execution_id": def_process_execution_id,
            "status": "RUNNING"
        }), 202
        
    except WorkflowError as e:
        return jsonify({"message": "Workflow initialization error", "error": str(e)}), 400
    except Exception as e:
        return jsonify({"message": "System error during startup", "error": str(e)}), 500
