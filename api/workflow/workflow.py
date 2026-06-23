"""
Workflow API Endpoints

CRUD for workflows.
"""

from flask import request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from utils.auth import role_required
from datetime import datetime
import os
from flask import current_app

from executors.extensions import db
from executors.models import DefProcess, DefAsyncTask
from workflow_engine.engine import WorkflowEngine, WorkflowError, ExecutionStatus
from workflow_engine.tasks import execute_workflow_task, resume_workflow_task
from workflow_engine.introspection import (
    introspect_inputs,
    introspect_outputs,
    batch_db_defined_inputs,
    build_predecessors,
    get_predecessor_outputs
)

from . import workflow_bp


@workflow_bp.route('/workflow', methods=['POST'])
@jwt_required()
@role_required()
def create_workflow():
    try:
        data = request.json
        process_name = data.get('process_name')
        
        # Accept nested 'process_structure' OR flat 'nodes'/'edges'
        process_structure = data.get('process_structure')
        if not process_structure and ('nodes' in data or 'edges' in data):
            process_structure = {
                "nodes": data.get('nodes', []),
                "edges": data.get('edges', [])
            }
        
        if not process_name:
            return jsonify({"error": "process_name is required"}), 400
            
        if not process_structure:
            return jsonify({"error": "Workflow structure is required"}), 400
        
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
@role_required()
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
        elif 'nodes' in data or 'edges' in data:
             # Support updating via flat payload
             current = workflow.process_structure or {"nodes": [], "edges": []}
             workflow.process_structure = {
                 "nodes": data.get('nodes', current.get('nodes', [])),
                 "edges": data.get('edges', current.get('edges', []))
             }
        
        workflow.last_updated_by = get_jwt_identity()
        workflow.last_update_date = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({"message": "Edited successfully", "result": workflow.json()}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error updating workflow", "error": str(e)}), 500


@workflow_bp.route('/workflow', methods=['GET'])
@jwt_required()
@role_required()
def get_all_workflows():
    try:
        process_id = request.args.get('process_id')
        process_name = request.args.get('process_name')
        
        if process_id:
            workflow = DefProcess.query.get(process_id)
            if not workflow:
                return jsonify({"error": "Workflow not found"}), 404
            return jsonify({"result": workflow.json()}), 200
            
        if process_name:
            workflow = DefProcess.query.filter_by(process_name=process_name).first()
            if not workflow:
                return jsonify({"error": "Workflow not found"}), 404
            return jsonify({"result": workflow.json()}), 200
            
        workflows = DefProcess.query.order_by(DefProcess.creation_date.desc()).all()
        return jsonify({"result": [w.json() for w in workflows]}), 200
    except Exception as e:
        return jsonify({"message": "Error fetching workflows", "error": str(e)}), 500


@workflow_bp.route('/workflow', methods=['DELETE'])
@jwt_required()
@role_required()
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
@role_required()
def validate_workflow():
    try:
        data = request.get_json(silent=True)
        if not data:
             return jsonify({"error": "No data provided"}), 400
             
        # Extract structure
        process_structure = data.get('process_structure')
        if not process_structure and ('nodes' in data or 'edges' in data):
             process_structure = {
                 "nodes": data.get('nodes', []),
                 "edges": data.get('edges', [])
             }
             
        if not process_structure:
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
@role_required()
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
        
        def _resolve_script_path(path_value, base_dir=None):
            if not path_value or not isinstance(path_value, str):
                return None
            
            # List of directories to check: provided base_dir, env var, and common fallback locations
            search_dirs = []
            if base_dir:
                search_dirs.append(base_dir)
            if script_base and script_base not in search_dirs:
                search_dirs.append(script_base)
            
            # Determine a dynamic fallback path relative to this file's location
            # (e.g., resolving from app/server/api/workflow to app/server/scripts/python)
            try:
                dynamic_fallback = os.path.abspath(os.path.join(current_app.root_path, 'scripts', 'python'))
            except Exception:
                dynamic_fallback = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'python'))
                
            fallback_dirs = [dynamic_fallback]
            for fd in fallback_dirs:
                if fd not in search_dirs:
                    search_dirs.append(fd)
            
            if path_value.endswith('.py') or os.path.sep in path_value or '/' in path_value:
                if os.path.isabs(path_value):
                    return path_value if os.path.isfile(path_value) else None
                for d in search_dirs:
                    full_path = os.path.join(d, path_value)
                    if os.path.isfile(full_path):
                        return full_path
                return path_value if os.path.isfile(path_value) else None
                
            # If no extension provided, add .py and search
            for d in search_dirs:
                auto_path = os.path.join(d, f"{path_value}.py")
                if os.path.isfile(auto_path):
                    return auto_path
            return None

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
                    # Dynamically determine script base from task if it looks like an ENV var (e.g. SCRIPT_PATH_01)
                    task_base = script_base
                    if t.script_path and not t.script_path.endswith('.py') and not os.path.isabs(t.script_path):
                        env_val = os.getenv(t.script_path.upper())
                        if env_val:
                            task_base = env_val
                    
                    path_val = t.script_path if (t.script_path and t.script_path.endswith('.py')) else t.script_name
                    resolved = _resolve_script_path(path_val, task_base)
                    if resolved:
                        script_path_map[t.task_name] = resolved
            except Exception as e:
                print(f"Error fetching tasks from DB: {e}")  # Fall back to auto-detection if DB lookup fails
            
            # Fallback: for tasks not found in DB, try auto-detecting {task_name}.py
            for task_name in unique_names:
                if task_name not in script_path_map:
                    resolved = _resolve_script_path(task_name, script_base)
                    if resolved:
                        script_path_map[task_name] = resolved
        
        
        # Batch fetch DB params
        db_params_map = batch_db_defined_inputs(task_names)
        
        # Cache for introspection
        script_cache = {}
        
        def _get_inputs(task_name, node_data):
            """Get inputs for a task - script introspection first, then fallback to DB metadata"""
            path = node_data.get('script_path') or node_data.get('script') or script_path_map.get(task_name)
            
            script_inputs = []
            if path:
                cache_key = f"in_{path}"
                if cache_key not in script_cache:
                    script_cache[cache_key] = introspect_inputs(path)
                script_inputs = script_cache[cache_key]
                
            db_params = db_params_map.get(task_name) or []
            
            if script_inputs:
                # Use script inputs as source of truth for WHAT is required
                # Use DB params to enrich with type and description
                db_lookup = {p['name']: p for p in db_params}
                result = []
                for sp in script_inputs:
                    name = sp['name']
                    if name in db_lookup:
                        result.append(db_lookup[name])
                    else:
                        result.append({
                            "name": name,
                            "type": "string",
                            "description": ""
                        })
                return result
                
            # Fallback to DB if script couldn't be introspected
            return db_params
        
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
            for param_meta in defined:
                param_name = param_meta['name']
                
                # Skip if already provided in node attributes (case-insensitive)
                if param_name.upper() in provided_dict:
                    continue
                
                # Skip if can be auto-filled from predecessor outputs (case-insensitive)
                if param_name.upper() in pred_output_keys:
                    continue
                
                # This input needs user input
                if param_name not in seen_inputs:
                    seen_inputs.add(param_name)
                    workflow_inputs.append({
                        "name": param_name,
                        "type": param_meta.get('type', 'string'),
                        "required": True,
                        "value": "",
                        "description": param_meta.get('description', ''),
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
@role_required()
def run_workflow(process_id):
    """
    Run a workflow asynchronously.
    Returns the execution_id immediately.
    """
    try:  
        context = {}
        data = request.get_json(silent=True)
        if data:
            context = data.get('context', {})
        
        engine = WorkflowEngine()
        user_id = get_jwt_identity()
        
        # 1. Initialize the execution record (Sync)
        def_process_execution_id = engine.initialize_execution(process_id, context, user_id)
        
        # 2. Run the engine via Celery Task
        execute_workflow_task.delay(def_process_execution_id)
        
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
@role_required()
def run_adhoc_workflow():
    """
    Run a workflow dynamically (ad-hoc) without requiring a saved process record.

    Optionally accepts a process_id to link the execution to an existing saved workflow.
    If process_id is omitted, the execution is stored with process_id=None.

    Request body:
    {
        "process_structure": {"nodes": [...], "edges": [...]},
        "context":           {"param1": "value1"},
        "process_id":        5   // optional — links execution to a saved workflow
    }

    Response:
    {
        "message": "Workflow started",
        "def_process_execution_id": 123,
        "status": "RUNNING"
    }
    """
    try:        
        data = request.get_json(silent=True)
        if not data or 'process_structure' not in data:
            return jsonify({"error": "process_structure is required"}), 400
        
        process_structure = data['process_structure']
        context = data.get('context', {})
        process_id = data.get('process_id')
        user_id = get_jwt_identity()
        
        engine = WorkflowEngine()
        
        # 1. Validate structure first
        errors = engine.validate(process_structure)
        if errors:
            return jsonify({"message": "Invalid workflow structure", "errors": errors}), 400
            
        # 2. Initialize execution — link to process if process_id provided
        def_process_execution_id = engine.initialize_execution(process_id, context, user_id)
        
        execute_workflow_task.delay(def_process_execution_id, process_structure=process_structure)
        
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


@workflow_bp.route('/workflow/predecessor_outputs', methods=['POST'])
@jwt_required()
@role_required()
def get_predecessor_outputs_api():
    """
    Get available output fields from predecessor nodes for a given decision node.
    """
    try:
        data = request.json or {}
        nodes = data.get('nodes', [])
        edges = data.get('edges', [])
        decision_node_id = data.get('decision_node_id')
        
        if not decision_node_id:
            return jsonify({"error": "decision_node_id is required"}), 400

        # Resolve task names to actual script paths so introspection works
        script_base = os.getenv("SCRIPT_PATH_01", "")

        def _resolve_script_path(path_value, base_dir=None):
            if not path_value or not isinstance(path_value, str):
                return None

            search_dirs = []
            if base_dir:
                search_dirs.append(base_dir)
            if script_base and script_base not in search_dirs:
                search_dirs.append(script_base)
            
            # Determine a dynamic fallback path relative to this file's location
            try:
                dynamic_fallback = os.path.abspath(os.path.join(current_app.root_path, 'scripts', 'python'))
            except Exception:
                dynamic_fallback = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'python'))
                
            fallback_dirs = [dynamic_fallback]
            for fd in fallback_dirs:
                if fd not in search_dirs:
                    search_dirs.append(fd)

            # Already a path-like value
            if path_value.endswith('.py') or os.path.sep in path_value or '/' in path_value:
                if os.path.isabs(path_value):
                    return path_value if os.path.isfile(path_value) else None
                for d in search_dirs:
                    full_path = os.path.join(d, path_value)
                    if os.path.isfile(full_path):
                        return full_path
                return path_value if os.path.isfile(path_value) else None

            # Task name fallback
            for d in search_dirs:
                auto_path = os.path.join(d, f"{path_value}.py")
                if os.path.isfile(auto_path):
                    return auto_path
            return None

        task_names = []
        for node in nodes:
            node_data = node.get('data', {}) or {}
            task_name = node_data.get('step_function') or node_data.get('task_name')
            if task_name:
                task_names.append(task_name)

        script_path_map = {}
        if task_names:
            unique_names = list(set(task_names))
            try:
                tasks = DefAsyncTask.query.filter(DefAsyncTask.task_name.in_(unique_names)).all()
                for task in tasks:
                    task_base = script_base
                    if task.script_path and not task.script_path.endswith('.py') and not os.path.isabs(task.script_path):
                        env_val = os.getenv(task.script_path.upper())
                        if env_val:
                            task_base = env_val

                    path_value = task.script_path if (task.script_path and task.script_path.endswith('.py')) else task.script_name
                    resolved = _resolve_script_path(path_value, task_base)
                    if resolved:
                        script_path_map[task.task_name] = resolved
            except Exception:
                pass

            # Filesystem fallback for unresolved names
            for task_name in unique_names:
                if task_name not in script_path_map:
                    resolved = _resolve_script_path(task_name, script_base)
                    if resolved:
                        script_path_map[task_name] = resolved

        resolved_nodes = []
        for node in nodes:
            node_copy = dict(node)
            node_data = dict(node.get('data', {}) or {})

            step_function = node_data.get('step_function')
            task_name = node_data.get('task_name')

            resolved = _resolve_script_path(step_function)
            if not resolved and task_name:
                resolved = script_path_map.get(task_name)
            if not resolved and step_function:
                resolved = script_path_map.get(step_function)

            if resolved:
                node_data['step_function'] = resolved

            node_copy['data'] = node_data
            resolved_nodes.append(node_copy)
            
        fields = get_predecessor_outputs(resolved_nodes, edges, decision_node_id)

        # Operators list — single source of truth (matches SAFE_OPERATORS in engine.py)
        operators = [
            {"label": "Equals (==)",      "value": "=="},
            {"label": "Not Equals (!=)",   "value": "!="},
            {"label": "Greater Than (>)",  "value": ">"},
            {"label": "Greater or Equal (>=)", "value": ">="},
            {"label": "Less Than (<)",     "value": "<"},
            {"label": "Less or Equal (<=)","value": "<="},
            {"label": "Contains",          "value": "contains"},
            {"label": "Not Contains",      "value": "not_contains"},
            {"label": "Is Empty",          "value": "is_empty"},
            {"label": "Is Not Empty",      "value": "is_not_empty"},
        ]
        
        return jsonify({"fields": fields, "operators": operators}), 200
        
    except Exception as e:
        return jsonify({"message": "Error introspecting predecessor outputs", "error": str(e)}), 500
