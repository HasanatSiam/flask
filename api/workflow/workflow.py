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
from workflow_engine.introspection import (
    introspect_inputs,
    introspect_outputs,
    batch_db_defined_inputs,
    build_predecessors
)

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


@workflow_bp.route('/workflow/required_params', methods=['POST'])
@jwt_required()
def get_required_params():
    """
    Get required USER parameters for a workflow - analyzes task dependencies.
    
    This scans scripts and excludes inputs that are auto-filled from predecessor outputs.
    Returns only the inputs the user must provide before running the workflow.
    
    Accepts graph format: {"nodes":[...],"edges":[...]}
    
    Response:
    {
      "workflow_inputs": [
        {"name": "user_id", "type": "string", "required": true, "source_task": "validate_user_id"}
      ],
      "has_required_inputs": true,
      "total_inputs": 1
    }
    """
    from executors.models import DefAsyncTask
    import os
    
    try:
        payload = request.get_json() or {}
        nodes = payload.get('nodes', [])
        edges = payload.get('edges', [])
        
        if not nodes:
            return jsonify({"error": "No nodes provided"}), 400
        
        # Build predecessor map
        preds = build_predecessors(nodes, edges)
        
        # Script base path from env, with hardcoded fallback
        script_base = os.getenv("SCRIPT_PATH_01", "")
        
        # Collect all task names to batch lookup script paths
        task_names = []
        for n in nodes:
            data = n.get('data', {}) or {}
            task_name = data.get('step_function') or data.get('task_name')
            if task_name:
                task_names.append(task_name)
        
        # Batch lookup script paths from DefAsyncTask
        script_path_map = {}
        if task_names:
            unique_names = list(set(task_names))
            try:
                tasks = DefAsyncTask.query.filter(DefAsyncTask.task_name.in_(unique_names)).all()
                for t in tasks:
                    # Prefer script_path, fallback to script_name
                    path = t.script_path or t.script_name
                    if path:
                        # If it's already an absolute path and exists, use it directly
                        if os.path.isabs(path):
                            if os.path.isfile(path):
                                script_path_map[t.task_name] = path
                        elif script_base:
                            # Join with script_base if available
                            full_path = os.path.join(script_base, path)
                            if os.path.isfile(full_path):
                                script_path_map[t.task_name] = full_path
                        else:
                            # No script_base, try path directly (might work if relative to CWD)
                            if os.path.isfile(path):
                                script_path_map[t.task_name] = path
            except Exception as e:
                print(f"Error fetching tasks from DB: {e}")  # Fall back to auto-detection if DB lookup fails
            
            # Fallback: for tasks not found in DB, try auto-detecting {task_name}.py
            if script_base:
                for task_name in unique_names:
                    if task_name not in script_path_map:
                        auto_path = os.path.join(script_base, f"{task_name}.py")
                        if os.path.isfile(auto_path):
                            script_path_map[task_name] = auto_path
        
        
        # Batch fetch DB params
        db_params_map = batch_db_defined_inputs(task_names)
        
        # Cache for introspection
        script_cache = {}
        
        def _get_inputs(task_name, node_data):
            """Get inputs for a task - DB first, then introspect script"""
            if db_params_map.get(task_name):
                return db_params_map[task_name]
            
            # Try node's script_path first, then lookup from DB
            path = node_data.get('script_path') or node_data.get('script') or script_path_map.get(task_name)
            if not path:
                return []
            
            cache_key = f"in_{path}"
            if cache_key not in script_cache:
                script_cache[cache_key] = introspect_inputs(path)
            return script_cache[cache_key]
        
        def _get_outputs(task_name, node_data):
            """Get outputs for a task - introspect from script"""
            path = node_data.get('script_path') or node_data.get('script') or script_path_map.get(task_name)
            if not path:
                return []
            
            cache_key = f"out_{path}"
            if cache_key not in script_cache:
                script_cache[cache_key] = introspect_outputs(path)
            return script_cache[cache_key]
        
        # Build node lookup maps
        node_map = {n['id']: n for n in nodes}
        
        # Introspect outputs for all nodes
        node_outputs = {}
        for n in nodes:
            node_id = n.get('id')
            data = n.get('data', {}) or {}
            task_name = data.get('step_function') or data.get('task_name')
            node_type = data.get('type', '')
            
            if node_type in ('Start', 'Stop') or not task_name:
                node_outputs[node_id] = []
            else:
                node_outputs[node_id] = _get_outputs(task_name, data)
        
        # Collect workflow-level required inputs
        # These are inputs that cannot be satisfied by any predecessor
        workflow_inputs = []
        seen_inputs = set()  # Avoid duplicates
        
        for n in nodes:
            node_id = n.get('id')
            data = n.get('data', {}) or {}
            task_name = data.get('step_function') or data.get('task_name')
            label = data.get('label') or task_name or 'Unknown'
            node_type = data.get('type', '')
            provided = data.get('attributes') or data.get('parameters') or {}
            
            # Skip Start/Stop nodes
            if node_type in ('Start', 'Stop') or not task_name:
                continue
            
            # Get defined inputs for this task
            defined = _get_inputs(task_name, data)
            if not defined:
                continue
            
            # Collect ALL predecessor output keys (recursive through chain)
            # Not just direct predecessors, but all ancestors
            pred_output_keys = set()
            visited = set()
            stack = list(preds.get(node_id, []))
            while stack:
                pred_id = stack.pop()
                if pred_id in visited:
                    continue
                visited.add(pred_id)
                # Normalize output keys to uppercase for case-insensitive matching
                pred_output_keys.update(k.upper() for k in node_outputs.get(pred_id, []))
                # Add predecessors of this predecessor
                stack.extend(preds.get(pred_id, []))
            
            # Convert provided to dict - handle both formats:
            # {name: x, value: y} or {attribute_name: x, attribute_value: y}
            # Keys are normalized to UPPERCASE for case-insensitive matching
            provided_dict = {}
            if isinstance(provided, dict):
                provided_dict = {k.upper(): v for k, v in provided.items()}
            elif isinstance(provided, list):
                for p in provided:
                    if isinstance(p, dict):
                        key = p.get('name') or p.get('attribute_name')
                        val = p.get('value') or p.get('attribute_value')
                        if key:
                            provided_dict[key.upper()] = val
            
            # Find inputs that are NOT satisfied by predecessors
            for param in defined:
                # Skip if already provided in node attributes (case-insensitive)
                if param.upper() in provided_dict:
                    continue
                
                # Skip if can be auto-filled from predecessor outputs (case-insensitive)
                if param.upper() in pred_output_keys:
                    continue
                
                # This input needs user input
                input_key = param  # Use param name as dedup key
                if input_key not in seen_inputs:
                    seen_inputs.add(input_key)
                    workflow_inputs.append({
                        "name": param,
                        "type": "string",
                        "required": True,
                        "value": "",
                        "source_task": task_name,
                        "source_label": label
                    })
        
        return jsonify({
            "workflow_inputs": workflow_inputs,
            "has_required_inputs": len(workflow_inputs) > 0,
            "total_inputs": len(workflow_inputs)
        }), 200
        
    except Exception as e:
        return jsonify({"error": "Failed to get parameters", "details": str(e)}), 500


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


@workflow_bp.route('/workflow/run_dynamic', methods=['POST'])
@jwt_required()
def run_adhoc_workflow():
    """
    Run an unsaved workflow asynchronously tracking execution history.
    
    Does NOT create a DefProcess record.
    The execution history (DefProcessExecution, DefProcessExecutionStep) will start
    with process_id=None, but will contain all step details.
    
    Request body:
    {
        "process_structure": {"nodes": [...], "edges": [...]},
        "context": {"param1": "value1"}
    }
    
    Response:
    {
        "message": "Workflow started",
        "def_process_execution_id": 123,
        "status": "RUNNING"
    }
    """
    try:
        from threading import Thread
        from flask import current_app
        
        data = request.get_json(silent=True)
        if not data or 'process_structure' not in data:
            return jsonify({"error": "process_structure is required"}), 400
        
        process_structure = data['process_structure']
        context = data.get('context', {})
        user_id = get_jwt_identity()
        
        engine = WorkflowEngine()
        
        # 1. Validate structure first
        errors = engine.validate(process_structure)
        if errors:
            return jsonify({"message": "Invalid workflow structure", "errors": errors}), 400
            
        # 2. Initialize execution without a Process ID (Ad-hoc run)
        # This creates the DefProcessExecution record to hold history
        def_process_execution_id = engine.initialize_execution(None, context, user_id)
        
        # 3. Run the engine (Async) passing the structure explicitly
        app = current_app._get_current_object()
        def background_run():
            with app.app_context():
                try:
                    # Pass the structure directly since it's not in DB
                    engine.execute_from_id(
                        def_process_execution_id, 
                        process_structure=process_structure
                    )
                except Exception as e:
                    print(f"Background adhoc execution failed for {def_process_execution_id}: {e}")

        Thread(target=background_run).start()
        
        return jsonify({
            "message": "Workflow started",
            "def_process_execution_id": def_process_execution_id,
            "status": "RUNNING"
        }), 202
        
    except WorkflowError as e:
        return jsonify({"message": "Workflow initialization error", "error": str(e)}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "System error during startup", "error": str(e)}), 500
