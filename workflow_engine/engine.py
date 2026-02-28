"""
Workflow Engine - Execute process definitions

Parses process_structure (nodes/edges) and executes step_functions
via DefAsyncTask executors.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable

from executors.extensions import db
from executors.models import DefAsyncTask, DefProcess, DefProcessNodeType, DefProcessExecution, DefProcessExecutionStep

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
            
        if behavior == 'GATEWAY':
            # Gateway logic (just pass for now, logic in _get_next_nodes)
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
                'error': f'Task not found: {step_function}'
            }
        
        # Get executor
        executor = EXECUTORS.get(task.executor)
        if not executor:
            return {
                'status': 'failed',
                'node_id': node_id,
                'error': f'Unknown executor: {task.executor}'
            }
        
        try:
            # Execute task with step_context (includes predefined attributes)
            eager_result = executor.apply(
                args=(
                    task.script_name or '',
                    task.user_task_name,
                    task.task_name,
                    None, None, None, None
                ),
                kwargs=step_context
            )
            
            # Get actual result from EagerResult
            executor_output = eager_result.get() if hasattr(eager_result, 'get') else eager_result
            
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
                    'error': error
                }

            return {
                'status': 'completed',
                'node_id': node_id,
                'task_name': task.task_name,
                'result': actual_result
            }
            
        except Exception as e:
            logger.exception(f"Step failed: {label}")
            return {
                'status': 'failed',
                'node_id': node_id,
                'error': str(e)
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

        context = execution_record.input_data or {}
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
                    status='RUNNING',
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

                if result.get('result') and isinstance(result['result'], dict):
                    context.update(result['result'])
                
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



    def _evaluate_condition(self, edge: dict, context: dict) -> bool:
        """Evaluate a single edge's condition against context."""
        condition = (edge.get('data') or {}).get('condition', {})
        
        # If it's the default branch, we handled it in _evaluate_decision (it's the fallback)
        if not condition or condition.get('is_default'):
            return False

        field = condition.get('field', '')
        operator = condition.get('operator', '')
        value = condition.get('value', '')
        
        # Manual input or Dropdown input - both result in a string key 'field'
        field_val = context.get(field)
        
        # If field is missing from context, usually default to empty string or None handling
        if field_val is None:
            field_val = ''

        op_fn = SAFE_OPERATORS.get(operator)
        if not op_fn:
            return False
            
        try:
            return op_fn(field_val, value)
        except (ValueError, TypeError):
            # Type mismatch (e.g. comparing string "abc" > 100) -> safely return False
            return False

    def _evaluate_decision(self, node: dict, context: dict) -> str:
        """Evaluate decision node, return target node ID."""
        edges = self._edges_by_source.get(node['id'], [])
        default_edge = None
        
        for edge in edges:
            condition = (edge.get('data') or {}).get('condition', {})
            
            # Check for default/fallback flag
            if condition.get('is_default'):
                default_edge = edge
                continue
            
            # Check explicit condition
            if self._evaluate_condition(edge, context):
                return edge['target']  # First match wins
        
        # Fallback priority:
        # 1. Marked default edge
        # 2. First edge in list (Robustness fallback)
        if default_edge:
            return default_edge['target']
        
        if edges:
            return edges[0]['target']
            
        raise Exception(f"Decision node '{node.get('data',{}).get('label')}' has no outgoing edges")

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
