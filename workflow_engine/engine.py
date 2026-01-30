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
            
            # Extract only the inner 'result' field
            actual_result = None
            if isinstance(executor_output, dict):
                actual_result = executor_output.get('result')
            else:
                actual_result = executor_output
            
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
    
    def initialize_execution(self, process_id: int, context: dict = None, user_id: int = None) -> int:
        """
        Create the execution record and return the ID immediately.
        """
        process = DefProcess.query.get(process_id)
        if not process:
            raise WorkflowError(f"Process not found: {process_id}")
            
        execution_record = DefProcessExecution(
            process_id=process_id,
            execution_status='RUNNING',
            input_data=context or {},
            execution_start_date=datetime.utcnow(),
            created_by=user_id,
            last_updated_by=user_id
        )
        db.session.add(execution_record)
        db.session.commit()
        return execution_record.def_process_execution_id

    def execute_from_id(self, execution_id: int, on_task_complete: Callable[[dict], None] = None):
        """
        Resume and run an execution from its ID.
        """
        execution_record = DefProcessExecution.query.get(execution_id)
        if not execution_record:
            raise WorkflowError(f"Execution record not found: {execution_id}")

        process = DefProcess.query.get(execution_record.process_id)
        context = execution_record.input_data or {}
        user_id = execution_record.created_by
        
        try:
            # Parse structure
            self._parse(process.process_structure)
            
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
                
                node_type_config = DefProcessNodeType.query.filter_by(shape_name=node.get('data', {}).get('type')).first()
                if node_type_config and node_type_config.behavior == 'EVENT' and node.get('data', {}).get('type') == 'Stop':
                    break 
                    
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
