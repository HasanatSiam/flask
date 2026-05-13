"""
Workflow Engine - Execute process definitions

Parses process_structure (nodes/edges) and executes step_functions
via DefAsyncTask executors.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable

from executors.extensions import db
from executors.models import DefAsyncTask, DefProcess, DefProcessNodeType, DefProcessExecution, DefProcessExecutionStep, DefAsyncTaskParam

# Import executors
from executors import run_script, bash_script, execute_procedure, execute_function, http_request

logger = logging.getLogger(__name__)


# Safe comparison operators for decision nodes
SAFE_OPERATORS = {
    '==':           lambda a, b: str(a).strip().lower() == str(b).strip().lower(),
    '!=':           lambda a, b: str(a).strip().lower() != str(b).strip().lower(),
    '>':            lambda a, b: float(a) > float(b),
    '>=':           lambda a, b: float(a) >= float(b),
    '<':            lambda a, b: float(a) < float(b),
    '<=':           lambda a, b: float(a) <= float(b),
    'contains':     lambda a, b: str(b).lower() in str(a).lower(),
    'not_contains': lambda a, b: str(b).lower() not in str(a).lower(),
    'is_empty':     lambda a, b: not a,
    'is_not_empty': lambda a, b: bool(a),
}


def normalize_boolean(val: Any) -> Any:
    """Normalize truthy/falsy values to standard booleans if possible."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        s = val.lower().strip()
        if s in ('true', 'yes', 'y', '1', 'on'):
            return True
        if s in ('false', 'no', 'n', '0', 'off'):
            return False
    if isinstance(val, (int, float)):
        if val == 1: return True
        if val == 0: return False
    return val


# Executor registry
EXECUTORS = {
    'executors.python.execute': run_script,
    'executors.bash.execute': bash_script,
    'executors.stored_procedure.execute': execute_procedure,
    'executors.stored_function.execute': execute_function,
    'executors.http.execute': http_request,
    # Short names
    'python': run_script,
    'bash': bash_script,
    'stored_procedure': execute_procedure,
    'stored_function': execute_function,
    'http': http_request,
}


class WorkflowError(Exception):
    """Workflow execution error"""
    pass


class WorkflowEngine:
    """
    Execute workflow process definitions.
    
    Usage:
        engine = WorkflowEngine()
        result = engine.run(process_id, context={})
    """
    
    def __init__(self):
        self._nodes: Dict[str, dict] = {}
        self._edges_by_source: Dict[str, List[dict]] = {}
    
    def _parse(self, process_structure: dict):
        """Parse nodes and edges into indexed structures."""
        nodes = process_structure.get('nodes', [])
        edges = process_structure.get('edges', [])
        
        # Index nodes by ID
        self._nodes = {n['id']: n for n in nodes}
        
        # Index edges by source
        self._edges_by_source = {}
        for edge in edges:
            src = edge['source']
            if src not in self._edges_by_source:
                self._edges_by_source[src] = []
            self._edges_by_source[src].append(edge)
    
    def _find_start(self) -> Optional[dict]:
        """Find the Start node."""
        for node in self._nodes.values():
            if node.get('data', {}).get('type') == 'Start':
                return node
        return None
    
    def _get_next_nodes(self, node_id: str) -> List[dict]:
        """Get nodes connected by outgoing edges."""
        edges = self._edges_by_source.get(node_id, [])
        next_nodes = []
        for edge in edges:
            target = self._nodes.get(edge['target'])
            if target:
                next_nodes.append(target)
        return next_nodes
    
    def _execute_step(self, node: dict, context: dict) -> dict:
        """Execute a single step node."""
        node_id = node['id']
        data = node.get('data', {})
        shape_name = data.get('type', '')
        step_function = data.get('step_function', '')
        label = data.get('label', node_id)
        
        # 1. Look up Behavior from DB
        node_type_config = DefProcessNodeType.query.filter_by(shape_name=shape_name).first()
        behavior = node_type_config.behavior if node_type_config else 'TASK' # Default to TASK if unknown

        logger.debug(f"Executing: {label} (shape={shape_name}, behavior={behavior})")

        # Extract predefined attributes from node and merge into context
        predefined_attributes = data.get('attributes', [])
        step_context = context.copy()  # Don't mutate original context
        
        for attr in predefined_attributes:
            if isinstance(attr, dict) and 'attribute_name' in attr:
                attr_name = attr['attribute_name']
                attr_value = attr.get('attribute_value', '')
                step_context[attr_name] = attr_value
                logger.debug(f"Node {label}: predefined attribute {attr_name}={attr_value}")
        
        # Behavior Logic
        if behavior == 'EVENT':
            return {'status': 'passed', 'node_id': node_id}
            
        if behavior == 'GATEWAY' and not step_function:
            # Simple gateway with no task logic
            return {'status': 'passed', 'node_id': node_id}

        # TASK logic -> continue to executors
        
        # Empty step_function - skip
        if not step_function:
            logger.debug(f"Skipping {label} - no step_function")
            return {'status': 'skipped', 'node_id': node_id}
        
        # Lookup task
        task = DefAsyncTask.query.filter_by(task_name=step_function).first()
        if not task:
            logger.warning(f"Task not found: {step_function}")
            return {
                'status': 'failed',
                'node_id': node_id,
                'error': f'Task not found: {step_function}',
                'input_data': step_context
            }
            
        # --- Strict Parameter Filtering ---
        expected_params = DefAsyncTaskParam.query.filter_by(task_name=task.task_name).all()
        
        strict_context = {}
        for param in expected_params:
            if param.parameter_name and param.parameter_name in step_context:
                strict_context[param.parameter_name] = step_context[param.parameter_name]
        
        # Get executor
        executor = EXECUTORS.get(task.executor)
        if not executor:
            return {
                'status': 'failed',
                'node_id': node_id,
                'error': f'Unknown executor: {task.executor}',
                'input_data': strict_context
            }
        
        try:
            # Execute task via Celery broker (logged in Redis/Flower)
            async_result = executor.apply_async(
                args=(
                    task.script_name or '',
                    task.user_task_name,
                    task.task_name,
                    None, None, None, None
                ),
                kwargs=strict_context
            )
            
            # Wait for worker result; propagate=False returns exceptions as values
            executor_output = async_result.get(propagate=False)
            
            logger.debug(f"Executor output for {label}: {executor_output}")

            # Extract result or error
            actual_result = None
            error = None
            
            if isinstance(executor_output, dict):
                actual_result = executor_output.get('result')
                error = executor_output.get('error')
            else:
                actual_result = executor_output
            
            if error:
                return {
                    'status': 'failed',
                    'node_id': node_id,
                    'error': error,
                    'input_data': strict_context
                }

            return {
                'status': 'completed',
                'node_id': node_id,
                'task_name': task.task_name,
                'result': actual_result,
                'input_data': strict_context
            }
            
        except Exception as e:
            logger.exception(f"Step failed: {label}")
            return {
                'status': 'failed',
                'node_id': node_id,
                'error': str(e),
                'input_data': strict_context
            }
    
    def initialize_execution(self, process_id: Optional[int], context: dict = None, user_id: int = None) -> int:
        """
        Create the execution record and return the ID immediately.
        Wrapper supports process_id=None for ad-hoc executions.
        """
        if process_id is not None:
            process = DefProcess.query.get(process_id)
            if not process:
                raise WorkflowError(f"Process not found: {process_id}")
            
        execution_record = DefProcessExecution(
            process_id=process_id,  # Can be None
            execution_status='RUNNING',
            input_data=context or {},
            execution_start_date=datetime.utcnow(),
            created_by=user_id,
            last_updated_by=user_id
        )
        db.session.add(execution_record)
        db.session.commit()
        return execution_record.def_process_execution_id
    
    def execute_from_id(self, execution_id: int, on_task_complete: Callable[[dict], None] = None,
                       process_structure: dict = None):
        """
        Resume and run an execution from its ID.
        If process_structure is provided (adhoc run), use it instead of DB lookup.
        """
        execution_record = DefProcessExecution.query.get(execution_id)
        if not execution_record:
            raise WorkflowError(f"Execution record not found: {execution_id}")

        context = dict(execution_record.input_data or {})
        user_id = execution_record.created_by
        
        # Determine stricture source
        structure_to_use = process_structure
        if not structure_to_use:
            # Fallback to DB process
            if execution_record.process_id:
                process = DefProcess.query.get(execution_record.process_id)
                if process:
                    structure_to_use = process.process_structure
        
        if not structure_to_use:
            raise WorkflowError("No workflow structure found for execution")

        try:
            # Parse structure
            self._parse(structure_to_use)
            
            # Find start
            start = self._find_start()
            if not start:
                raise WorkflowError("No Start node found")

            current_nodes = [start]
            
            while current_nodes:
                node = current_nodes.pop(0)
                
                # Create step record (RUNNING)
                step_start_date = datetime.utcnow()
                step_record = DefProcessExecutionStep(
                    def_process_execution_id=execution_record.def_process_execution_id,
                    node_id=node.get('id'),
                    node_label=node.get('data', {}).get('label', node.get('id')),
                    task_name=node.get('data', {}).get('step_function', ''),
                    status='RUNNING',
                    input_data=context.copy() if isinstance(context, dict) else context,
                    execution_start_date=step_start_date,
                    created_by=user_id,
                    last_updated_by=user_id
                )
                db.session.add(step_record)
                db.session.commit()

                # Execute step
                try:
                    result = self._execute_step(node, context)
                except Exception as e:
                    result = {'status': 'failed', 'error': str(e), 'node_id': node.get('id')}

                step_end_date = datetime.utcnow()

                # Update step record
                step_record.status = result.get('status')
                if 'input_data' in result:
                    step_record.input_data = result.get('input_data')
                if result.get('status') == 'completed':
                    step_record.result = result.get('result')
                step_record.error_message = result.get('error')
                step_record.execution_end_date = step_end_date
                
                db.session.commit()
            

                
                if on_task_complete:
                    on_task_complete(result)
                
                if result.get('status') == 'failed':
                    execution_record.execution_status = 'FAILED'
                    execution_record.execution_end_date = datetime.utcnow()
                    execution_record.error_message = result.get('error')
                    db.session.commit()
                    return

                step_result = result.get('result')
                if step_result is not None:
                    # If result is a JSON string, try to parse it, but don't lose the original if it fails
                    if isinstance(step_result, str):
                        try:
                            parsed_result = json.loads(step_result)
                            if isinstance(parsed_result, (dict, list, bool, int, float)):
                                step_result = parsed_result
                        except (ValueError, TypeError):
                            pass
                    
                    if isinstance(step_result, dict):
                        context.update(step_result)
                    else:
                        # Normalize and store simple results
                        norm_val = normalize_boolean(step_result)
                        context['last_result'] = norm_val
                        context[f"{node.get('id')}_result"] = norm_val
                
                # Check if we need to break for Stop node
                node_type_config = DefProcessNodeType.query.filter_by(shape_name=node.get('data', {}).get('type')).first()
                if node_type_config and node_type_config.behavior == 'EVENT' and node.get('data', {}).get('type') == 'Stop':
                    break 
                    
                # Decision / Gateway Logic
                if node_type_config and node_type_config.behavior == 'GATEWAY':
                    try:
                        target_id = self._evaluate_decision(node, context)
                        target_node = self._nodes.get(target_id)
                        if target_node:
                            current_nodes.append(target_node)
                        else:
                            execution_record.error_message = f"Gateway target node '{target_id}' not found"
                            execution_record.execution_status = 'FAILED'
                            db.session.commit()
                            return
                    except Exception as e:
                        execution_record.error_message = f"Gateway evaluation error: {str(e)}"
                        execution_record.execution_status = 'FAILED'
                        db.session.commit()
                        return
                else:
                    # Default linear flow
                    next_nodes = self._get_next_nodes(node['id'])
                    if next_nodes:
                        current_nodes.append(next_nodes[0])
            
            # Success
            execution_record.execution_status = 'COMPLETED'
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.output_data = context
            db.session.commit()
            
        except Exception as e:
            db.session.rollback()
            execution_record.execution_status = 'FAILED'
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.error_message = str(e)
            db.session.commit()
            raise



    def _evaluate_node_condition(self, field: str, operator: str, value: str, context: dict) -> str:
        """
        Evaluate a node-level binary condition against the execution context.

        Returns:
            'true'    - operator evaluated cleanly and result is True
            'false'   - operator evaluated cleanly and result is False
            'default' - field missing, unknown operator, or type mismatch
        """
        if field not in context:
            logger.debug(f"Binary condition: field '{field}' not in context -> default")
            return 'default'

        op_fn = SAFE_OPERATORS.get(operator)
        if not op_fn:
            logger.debug(f"Binary condition: unknown operator '{operator}' -> default")
            return 'default'

        field_val = context.get(field)
        norm_field_val = normalize_boolean(field_val)
        norm_target_val = normalize_boolean(value)

        if norm_field_val is None:
            norm_field_val = ''

        try:
            if isinstance(norm_field_val, bool) and isinstance(norm_target_val, bool):
                if operator == '==':
                    result = norm_field_val == norm_target_val
                elif operator == '!=':
                    result = norm_field_val != norm_target_val
                else:
                    return 'default'
            else:
                result = op_fn(norm_field_val, norm_target_val)
            return 'true' if result else 'false'
        except (ValueError, TypeError):
            logger.debug(f"Binary condition: type mismatch for field '{field}' -> default")
            return 'default'

    def _evaluate_decision(self, node: dict, context: dict) -> str:
        """
        Evaluate a decision (GATEWAY) node and return the target node ID.

        Mode detection (by key present in node.data):
          'condition'    -> Binary mode   : evaluates field+operator+value -> true/false/default
          'switch_field' -> Switch/case   : reads field value, matches against edge case_value
          neither        -> WorkflowError : node is misconfigured

        Edge routing priority (both modes):
          1. Edge whose branch/case_value matches the outcome
          2. Edge marked is_default=true  (catch-all fallback)
          3. edges[0]                     (last-resort robustness)
          4. No edges at all              -> raise WorkflowError
        """
        edges = self._edges_by_source.get(node['id'], [])
        data = node.get('data', {}) or {}
        label = data.get('label', node.get('id'))

        if not edges:
            raise WorkflowError(f"Decision node '{label}' has no outgoing edges")

        # --- MODE 1: Binary ---
        if 'condition' in data:
            condition = data['condition'] or {}
            field = condition.get('field', '')
            operator = condition.get('operator', '')
            value = condition.get('value', '')

            outcome = self._evaluate_node_condition(field, operator, value, context)
            logger.debug(f"Binary decision '{label}': {field} {operator} {value!r} -> {outcome}")

            default_edge = None
            for edge in edges:
                edge_data = edge.get('data') or {}
                if edge_data.get('is_default'):
                    default_edge = edge
                    continue
                if str(edge_data.get('branch', '')).strip().lower() == outcome:
                    return edge['target']

            if default_edge:
                return default_edge['target']
            return edges[0]['target']

        # --- MODE 2: Switch/case ---
        if 'switch_field' in data:
            switch_field = data['switch_field']
            actual_val = str(context.get(switch_field, '')).strip().lower()
            logger.debug(f"Switch/case decision '{label}': {switch_field}={actual_val!r}")

            default_edge = None
            for edge in edges:
                edge_data = edge.get('data') or {}
                if edge_data.get('is_default'):
                    default_edge = edge
                    continue
                case_val = str(edge_data.get('case_value', '')).strip().lower()
                if case_val and case_val == actual_val:
                    return edge['target']

            if default_edge:
                return default_edge['target']
            return edges[0]['target']

        # --- NEITHER: misconfigured ---
        raise WorkflowError(
            f"Decision node '{label}' must have 'condition' (binary mode) "
            f"or 'switch_field' (switch/case mode) in its data"
        )

    def run(self, process_id: int, context: dict = None, user_id: int = None,
            on_task_complete: Callable[[dict], None] = None) -> dict:
        """
        Synchronous run for backward compatibility.
        """
        execution_id = self.initialize_execution(process_id, context, user_id)
        self.execute_from_id(execution_id, on_task_complete)
        
        execution = DefProcessExecution.query.get(execution_id)
        return execution.json()

    
    def validate(self, process_structure: dict) -> List[str]:
        """Validate process structure."""
        errors = []
        try:
            self._parse(process_structure)
            if not self._find_start():
                errors.append("No Start node found")
        except Exception as e:
            errors.append(f"Structure error: {str(e)}")
        return errors



    
def run_workflow(process_id: int, context: dict = None,
                 on_task_complete: Callable[[dict], None] = None) -> dict:
    """Convenience function to run a workflow."""
    engine = WorkflowEngine()
    return engine.run(process_id, context, on_task_complete)
