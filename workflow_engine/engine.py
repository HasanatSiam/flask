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


# Executor registry
EXECUTORS = {
    'executors.python.execute': run_script,
    'executors.bash.execute': bash_script,
    'executors.stored_procedure.execute': execute_procedure,
    'executors.stored_function.execute': execute_function,
    'executors.http.execute': http_request,
    'python': run_script,
    'bash': bash_script,
    'stored_procedure': execute_procedure,
    'stored_function': execute_function,
    'http': http_request,
}


class NodeBehavior:
    TASK = 'TASK'
    GATEWAY = 'GATEWAY'
    EVENT = 'EVENT'


class ExecutionStatus:
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


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

    def _parse(self, process_structure: dict) -> None:
        """Parse nodes and edges into indexed structures."""
        nodes = process_structure.get('nodes', [])
        edges = process_structure.get('edges', [])

        self._nodes = {n['id']: n for n in nodes}

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

    def _get_behavior(self, shape_name: str) -> str:
        """Look up node behavior from DB."""
        node_type_config = DefProcessNodeType.query.filter_by(shape_name=shape_name).first()
        return node_type_config.behavior if node_type_config else NodeBehavior.TASK

    def _inject_attributes(self, node: dict, context: dict) -> dict:
        """Merge predefined node attributes into a copy of context."""
        step_context = context.copy()
        for attr in node.get('data', {}).get('attributes', []):
            if isinstance(attr, dict) and 'attribute_name' in attr:
                step_context[attr['attribute_name']] = attr.get('attribute_value', '')
        return step_context

    def _execute_step(self, node: dict, context: dict) -> dict:
        """Execute a single step node."""
        node_id = node['id']
        data = node.get('data', {})
        shape_name = data.get('type', '')
        step_function = data.get('step_function', '')
        label = data.get('label', node_id)

        behavior = self._get_behavior(shape_name)
        logger.debug(f"Executing: {label} (shape={shape_name}, behavior={behavior})")

        step_context = self._inject_attributes(node, context)

        # EVENT nodes (Start/Stop) — no task execution
        if behavior == NodeBehavior.EVENT:
            return {'status': 'passed', 'node_id': node_id}

        # GATEWAY with no step_function — pure routing node
        if behavior == NodeBehavior.GATEWAY and not step_function:
            return {'status': 'passed', 'node_id': node_id}

        # No step_function — skip
        if not step_function:
            logger.debug(f"Skipping {label} - no step_function")
            return {'status': 'skipped', 'node_id': node_id}

        # Lookup task from DB
        task = DefAsyncTask.query.filter_by(task_name=step_function).first()
        if not task:
            logger.warning(f"Task not found: {step_function}")
            return {
                'status': ExecutionStatus.FAILED,
                'node_id': node_id,
                'error': f'Task not found: {step_function}',
                'input_data': step_context
            }

        # Strict parameter filtering
        expected_params = DefAsyncTaskParam.query.filter_by(task_name=task.task_name).all()
        strict_context = {
            param.parameter_name: step_context[param.parameter_name]
            for param in expected_params
            if param.parameter_name and param.parameter_name in step_context
        }

        # Get executor
        executor = EXECUTORS.get(task.executor)
        if not executor:
            return {
                'status': ExecutionStatus.FAILED,
                'node_id': node_id,
                'error': f'Unknown executor: {task.executor}',
                'input_data': strict_context
            }

        try:
            async_result = executor.apply_async(
                args=(
                    task.script_name or '',
                    task.user_task_name,
                    task.task_name,
                    None, None, None, None
                ),
                kwargs=strict_context
            )

            executor_output = async_result.get(timeout=300, propagate=False)
            logger.debug(f"Executor output for {label}: {executor_output}")

            actual_result = None
            error = None

            if isinstance(executor_output, dict):
                actual_result = executor_output.get('result')
                error = executor_output.get('error')
            else:
                actual_result = executor_output

            # Executor-level error (celery/wrapper failed)
            if error:
                return {
                    'status': ExecutionStatus.FAILED,
                    'node_id': node_id,
                    'error': error,
                    'input_data': strict_context
                }

            # Script-level semantic error: the script itself returned {"error": "..."}
            # This is distinct from predictable SF scalars ("Y"/"N"/"E") which are strings, not dicts.
            if isinstance(actual_result, dict) and actual_result.get('error'):
                return {
                    'status': ExecutionStatus.FAILED,
                    'node_id': node_id,
                    'error': actual_result.get('error'),
                    'result': actual_result,
                    'input_data': strict_context
                }

            return {
                'status': ExecutionStatus.COMPLETED,
                'node_id': node_id,
                'task_name': task.task_name,
                'sf_type': task.sf_type or '',
                'result': actual_result,
                'input_data': strict_context
            }

        except Exception as e:
            logger.exception(f"Step failed: {label}")
            return {
                'status': ExecutionStatus.FAILED,
                'node_id': node_id,
                'error': str(e),
                'input_data': strict_context
            }

    def _update_context(self, node_id: str, step_result: Any, sf_type: str, context: dict) -> None:
        """Merge step result into context."""
        if step_result is None:
            return

        # Parse JSON string if needed
        if isinstance(step_result, str):
            try:
                parsed = json.loads(step_result)
                if isinstance(parsed, dict):
                    step_result = parsed
            except (ValueError, TypeError):
                pass

        if isinstance(step_result, dict):
            context.update(step_result)
        else:
            # Scalar result — store as-is (lookup value codes: Y/N/E etc.)
            context['predictable_result'] = step_result
            context[f"{node_id}_result"] = step_result

    def initialize_execution(self, process_id: Optional[int], context: dict = None, user_id: int = None) -> int:
        """Create the execution record and return the ID."""
        if process_id is not None:
            process = db.session.get(DefProcess, process_id)
            if not process:
                raise WorkflowError(f"Process not found: {process_id}")

        execution_record = DefProcessExecution(
            process_id=process_id,
            execution_status=ExecutionStatus.RUNNING,
            input_data=context or {},
            created_by=user_id,
            last_updated_by=user_id
        )
        db.session.add(execution_record)
        db.session.commit()
        return execution_record.def_process_execution_id

    def execute_from_id(self, execution_id: int, on_task_complete: Callable[[dict], None] = None,
                        process_structure: dict = None) -> None:
        """Resume and run an execution from its ID."""
        execution_record = db.session.get(DefProcessExecution, execution_id)
        if not execution_record:
            raise WorkflowError(f"Execution record not found: {execution_id}")

        context = dict(execution_record.input_data or {})
        user_id = execution_record.created_by

        structure_to_use = process_structure
        if not structure_to_use and execution_record.process_id:
            process = db.session.get(DefProcess, execution_record.process_id)
            if process:
                structure_to_use = process.process_structure

        if not structure_to_use:
            raise WorkflowError("No workflow structure found for execution")

        try:
            self._parse(structure_to_use)

            start = self._find_start()
            if not start:
                raise WorkflowError("No Start node found")

            current_nodes = [start]

            while current_nodes:
                node = current_nodes.pop(0)
                shape_name = node.get('data', {}).get('type', '')
                behavior = self._get_behavior(shape_name)

                # Create step record
                step_record = DefProcessExecutionStep(
                    def_process_execution_id=execution_record.def_process_execution_id,
                    node_id=node.get('id'),
                    node_label=node.get('data', {}).get('label', node.get('id')),
                    task_name=node.get('data', {}).get('step_function', ''),
                    status=ExecutionStatus.RUNNING,
                    input_data=context.copy(),
                    created_by=user_id,
                    last_updated_by=user_id
                )
                db.session.add(step_record)
                db.session.commit()

                # Execute step
                try:
                    result = self._execute_step(node, context)
                except Exception as e:
                    result = {'status': ExecutionStatus.FAILED, 'error': str(e), 'node_id': node.get('id')}

                # Update step record
                step_record.status = result.get('status')
                if 'input_data' in result:
                    step_record.input_data = result.get('input_data')
                if result.get('status') == ExecutionStatus.COMPLETED:
                    step_record.result = result.get('result')
                step_record.error_message = result.get('error')
                step_record.execution_end_date = datetime.utcnow()
                db.session.commit()

                if on_task_complete:
                    on_task_complete(result)

                # Stop execution on failure
                if result.get('status') == ExecutionStatus.FAILED:
                    execution_record.execution_status = ExecutionStatus.FAILED
                    execution_record.execution_end_date = datetime.utcnow()
                    execution_record.error_message = result.get('error')
                    db.session.commit()
                    return

                # Update context with step result
                # NOTE: predictable_result must be set BEFORE _evaluate_decision is called
                self._update_context(
                    node_id=node.get('id'),
                    step_result=result.get('result'),
                    sf_type=result.get('sf_type', ''),
                    context=context
                )

                # Stop node — end execution
                if behavior == NodeBehavior.EVENT and shape_name == 'Stop':
                    break

                # Gateway — evaluate decision
                if behavior == NodeBehavior.GATEWAY:
                    try:
                        target_id = self._evaluate_decision(node, context)
                        target_node = self._nodes.get(target_id)
                        if target_node:
                            current_nodes.append(target_node)
                        else:
                            execution_record.error_message = f"Gateway target node '{target_id}' not found"
                            execution_record.execution_status = ExecutionStatus.FAILED
                            db.session.commit()
                            return
                    except Exception as e:
                        execution_record.error_message = f"Gateway evaluation error: {str(e)}"
                        execution_record.execution_status = ExecutionStatus.FAILED
                        db.session.commit()
                        return
                else:
                    # Linear flow
                    next_nodes = self._get_next_nodes(node['id'])
                    if len(next_nodes) > 1:
                        logger.warning(f"Node '{node['id']}' has {len(next_nodes)} outgoing edges — only first will be followed")
                    if next_nodes:
                        current_nodes.append(next_nodes[0])

            # Completed
            execution_record.execution_status = ExecutionStatus.COMPLETED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.output_data = context
            db.session.commit()

        except Exception as e:
            db.session.rollback()
            execution_record.execution_status = ExecutionStatus.FAILED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.error_message = str(e)
            db.session.commit()
            raise

    def _evaluate_decision(self, node: dict, context: dict) -> str:
        edges = self._edges_by_source.get(node['id'], [])
        label = node.get('data', {}).get('label', node.get('id'))

        if not edges:
            raise WorkflowError(f"Decision node '{label}' has no outgoing edges")

        actual_val = str(context.get('predictable_result', '')).strip().lower()
        logger.debug(f"Decision '{label}': predictable_result={actual_val!r}")

        for edge in edges:
            edge_data = edge.get('data') or {}
            lookup_val = str(edge_data.get('lookup_value', '')).strip().lower()
            if lookup_val and lookup_val == actual_val:
                return edge['target']

        raise WorkflowError(f"Decision '{label}': no matching edge for '{actual_val}'")

    def run(self, process_id: int, context: dict = None, user_id: int = None,
            on_task_complete: Callable[[dict], None] = None) -> dict:
        """Synchronous run for backward compatibility."""
        execution_id = self.initialize_execution(process_id, context, user_id)
        self.execute_from_id(execution_id, on_task_complete)
        execution = db.session.get(DefProcessExecution, execution_id)
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


def run_workflow(process_id: int, context: dict = None, user_id: int = None,
                 on_task_complete: Callable[[dict], None] = None) -> dict:
    """Convenience function to run a workflow."""
    engine = WorkflowEngine()
    return engine.run(process_id, context, user_id, on_task_complete)