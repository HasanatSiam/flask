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
    WAITING_ON_TASK = 'WAITING_ON_TASK'


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
        """Initialize empty graph indexes.

        _nodes maps node_id -> node dict; _edges_by_source maps
        source_node_id -> list of edge dicts. Both are populated
        by _parse() before each execution.
        """
        self._nodes: Dict[str, dict] = {}
        self._edges_by_source: Dict[str, List[dict]] = {}

    def _parse(self, process_structure: dict) -> None:
        """Build in-memory indexes from the raw JSON graph.

        Converts the nodes/edges lists into dictionaries keyed by
        node ID for O(1) lookups during graph traversal.
        """
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
        """Locate the single Start node in the parsed graph.

        Scans all indexed nodes for one with type == 'Start'.
        Returns None if missing (validation error).
        """
        for node in self._nodes.values():
            if node.get('data', {}).get('type') == 'Start':
                return node
        return None

    def _get_next_nodes(self, node_id: str) -> List[dict]:
        """Return the node dicts reachable via outgoing edges from node_id."""
        edges = self._edges_by_source.get(node_id, [])
        next_nodes = []
        for edge in edges:
            target = self._nodes.get(edge['target'])
            if target:
                next_nodes.append(target)
        return next_nodes

    def _get_behavior(self, shape_name: str) -> str:
        """Determine node behavior (TASK/GATEWAY/EVENT) from the DefProcessNodeType table.

        Falls back to NodeBehavior.TASK if no configuration is found
        for the given shape_name.
        """
        node_type_config = DefProcessNodeType.query.filter_by(shape_name=shape_name).first()
        return node_type_config.behavior if node_type_config else NodeBehavior.TASK

    def _inject_attributes(self, node: dict, context: dict) -> dict:
        """Merge node-level static attributes into a copy of the execution context.

        Each node can define hardcoded attribute_name/attribute_value pairs
        at design time. These are layered on top of the runtime context so
        the step sees both.
        """
        step_context = context.copy()
        for attr in node.get('data', {}).get('attributes', []):
            if isinstance(attr, dict) and 'attribute_name' in attr:
                step_context[attr['attribute_name']] = attr.get('attribute_value', '')
        return step_context

    def _execute_step_async(self, node: dict, context: dict) -> dict:
        """Dispatch a single workflow node to its Celery executor.

        Resolves step_function -> DefAsyncTask -> executor, filters
        context to only declared parameters, injects engine plumbing
        keys (execution_id, current_node_id), and calls the executor's
        apply_async with a Celery link callback for automatic
        advancement on completion. Returns DISPATCHED, passed,
        skipped, or FAILED.
        """
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
        strict_context['execution_id']    = getattr(self, '_execution_id', None)
        strict_context['current_node_id'] = node_id

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
            # Lazy import to avoid circular dependency (tasks.py imports engine.py)
            from workflow_engine.tasks import advance_workflow_step

            async_result = executor.apply_async(
                args=(
                    task.script_name or '',
                    task.user_task_name,
                    task.task_name,
                    None, None, None, None
                ),
                kwargs=strict_context,
                link=advance_workflow_step.s(
                    execution_id=self._execution_id,
                    node_id=node_id
                )
            )

            return {
                'status': 'DISPATCHED',
                'node_id': node_id,
                'celery_task_id': async_result.id,
                'task_name': task.task_name,
                'sf_type': task.sf_type or '',
                'input_data': strict_context
            }

        except Exception as e:
            logger.exception(f"Failed to dispatch step: {label}")
            return {
                'status': ExecutionStatus.FAILED,
                'node_id': node_id,
                'error': str(e),
                'input_data': strict_context
            }

    def _update_context(self, node_id: str, step_result: Any, sf_type: str, context: dict) -> None:
        """Merge a completed step's result into the execution context.

        If result is a JSON string, attempts to parse it as a dict.
        Dict results are merged key-by-key; scalars are stored under
        both predictable_result and {node_id}_result. Also ensures
        {node_id}_result exists for gateway evaluation.
        """
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
            if f"{node_id}_result" not in step_result:
                node_result = (
                    step_result.get('result')
                    or step_result.get('output')
                    or step_result.get('status')
                )
                if node_result is not None:
                    context[f"{node_id}_result"] = node_result
        else:
            context['predictable_result'] = step_result
            context[f"{node_id}_result"] = step_result

    def initialize_execution(self, process_id: Optional[int], context: dict = None, user_id: int = None,
                             process_structure: dict = None) -> int:
        """Create a DefProcessExecution record and return its primary key.

        Looks up the DefProcess if only process_id is given, then
        persists the initial context and process_structure JSON so
        async callbacks can reload them. Status starts as RUNNING.
        """
        if process_id is not None:
            process = db.session.get(DefProcess, process_id)
            if not process:
                raise WorkflowError(f"Process not found: {process_id}")
            if not process_structure:
                process_structure = process.process_structure

        execution_record = DefProcessExecution(
            process_id=process_id,
            execution_status=ExecutionStatus.RUNNING,
            input_data=context or {},
            process_structure=process_structure,
            created_by=user_id,
            last_updated_by=user_id
        )
        db.session.add(execution_record)
        db.session.commit()
        return execution_record.def_process_execution_id

    def execute_from_id(self, execution_id: int, on_task_complete: Callable[[dict], None] = None,
                        process_structure: dict = None) -> None:
        """Dispatch the first (or next) step of an execution, then return.

        The Celery worker is only occupied for the dispatch logic (milliseconds).
        Subsequent steps are chained via Celery's link callback.
        """
        execution_record = db.session.get(DefProcessExecution, execution_id)
        if not execution_record:
            raise WorkflowError(f"Execution record not found: {execution_id}")

        context = dict(execution_record.input_data or {})
        user_id = execution_record.created_by
        self._execution_id = execution_id

        # Load structure from execution record, then fallback to parameter, then DB
        structure_to_use = execution_record.process_structure or process_structure
        if not structure_to_use and execution_record.process_id:
            process = db.session.get(DefProcess, execution_record.process_id)
            if process:
                structure_to_use = process.process_structure

        if not structure_to_use:
            raise WorkflowError("No workflow structure found for execution")

        self._parse(structure_to_use)

        # Ensure structure is saved for async callbacks
        if not execution_record.process_structure:
            execution_record.process_structure = structure_to_use
            db.session.commit()

        # Determine starting node
        if execution_record.execution_status == ExecutionStatus.WAITING_ON_TASK and execution_record.current_node_id:
            current_node_id = execution_record.current_node_id
            current_node = self._nodes.get(current_node_id)

            execution_record.execution_status = ExecutionStatus.RUNNING
            execution_record.current_node_id = None
            db.session.commit()

            if current_node:
                shape_name = current_node.get('data', {}).get('type', '')
                behavior = self._get_behavior(shape_name)
                if behavior == NodeBehavior.GATEWAY:
                    target_id = self._evaluate_decision(current_node, context)
                    target_node = self._nodes.get(target_id)
                    current_nodes = [target_node] if target_node else []
                else:
                    current_nodes = self._get_next_nodes(current_node_id)
            else:
                current_nodes = self._get_next_nodes(current_node_id)
        else:
            start = self._find_start()
            if not start:
                raise WorkflowError("No Start node found")
            current_nodes = [start]

        if not current_nodes:
            execution_record.execution_status = ExecutionStatus.COMPLETED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.output_data = context
            db.session.commit()
            return

        # Process nodes synchronously until we hit one that dispatches asynchronously
        while current_nodes:
            node = current_nodes.pop(0)
            node_id = node['id']

            # Create step record
            step_record = DefProcessExecutionStep(
                def_process_execution_id=execution_record.def_process_execution_id,
                node_id=node_id,
                node_label=node.get('data', {}).get('label', node_id),
                task_name=node.get('data', {}).get('step_function', ''),
                status=ExecutionStatus.RUNNING,
                input_data=context.copy(),
                created_by=user_id,
                last_updated_by=user_id
            )
            db.session.add(step_record)
            db.session.commit()

            # Dispatch step
            try:
                result = self._execute_step_async(node, context)
            except Exception as e:
                result = {'status': ExecutionStatus.FAILED, 'error': str(e), 'node_id': node_id}

            if on_task_complete:
                on_task_complete(result)

            status = result.get('status')

            # Pass-through nodes (no async dispatch) — process synchronously and continue
            if status in ('passed', 'skipped'):
                step_record.status = status
                step_record.execution_end_date = datetime.utcnow()
                db.session.commit()

                shape_name = node.get('data', {}).get('type', '')
                behavior = self._get_behavior(shape_name)

                # Stop node — end execution
                if behavior == NodeBehavior.EVENT and shape_name == 'Stop':
                    execution_record.execution_status = ExecutionStatus.COMPLETED
                    execution_record.execution_end_date = datetime.utcnow()
                    execution_record.output_data = context
                    db.session.commit()
                    return

                # Gateway — evaluate decision
                if behavior == NodeBehavior.GATEWAY:
                    try:
                        target_id = self._evaluate_decision(node, context)
                        target_node = self._nodes.get(target_id)
                        if target_node:
                            current_nodes.append(target_node)
                            continue
                        else:
                            execution_record.error_message = f"Gateway target node '{target_id}' not found"
                    except Exception as e:
                        execution_record.error_message = f"Gateway evaluation error: {str(e)}"
                    execution_record.execution_status = ExecutionStatus.FAILED
                    execution_record.execution_end_date = datetime.utcnow()
                    db.session.commit()
                    return

                # Linear flow
                next_nodes = self._get_next_nodes(node_id)
                if not next_nodes:
                    execution_record.execution_status = ExecutionStatus.COMPLETED
                    execution_record.execution_end_date = datetime.utcnow()
                    execution_record.output_data = context
                    db.session.commit()
                    return

                current_nodes.append(next_nodes[0])
                continue

            # DISPATCHED — async task sent to broker
            if status == 'DISPATCHED':
                step_record.celery_task_id = result.get('celery_task_id')
                step_record.status = 'DISPATCHED'
                if 'input_data' in result:
                    step_record.input_data = result.get('input_data')
                db.session.commit()
                return

            # FAILED during dispatch
            step_record.status = ExecutionStatus.FAILED
            step_record.error_message = result.get('error')
            if 'input_data' in result:
                step_record.input_data = result.get('input_data')
            step_record.execution_end_date = datetime.utcnow()
            db.session.commit()

            execution_record.execution_status = ExecutionStatus.FAILED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.error_message = result.get('error')
            db.session.commit()
            return

        # All nodes exhausted
        execution_record.execution_status = ExecutionStatus.COMPLETED
        execution_record.execution_end_date = datetime.utcnow()
        execution_record.output_data = context
        db.session.commit()

    def _complete_step_and_advance(self, execution_id: int, node_id: str, executor_result: Any) -> None:
        """Celery callback: process a completed step and advance the workflow.

        Runs inside the advance_workflow_step Celery task. Updates the step
        record with the executor result, merges it into the execution context,
        then calls _dispatch_next_nodes to continue graph traversal.
        """
        execution_record = db.session.get(DefProcessExecution, execution_id)
        if not execution_record:
            logger.error(f"Execution not found for advance: {execution_id}")
            return

        step = DefProcessExecutionStep.query.filter_by(
            def_process_execution_id=execution_id,
            node_id=node_id
        ).order_by(DefProcessExecutionStep.execution_start_date.desc()).first()

        if not step:
            logger.error(f"Step not found for advance: exec={execution_id}, node={node_id}")
            return

        context = dict(execution_record.input_data or {})
        self._execution_id = execution_id

        # Parse graph if not already loaded
        if not self._nodes:
            structure = self._load_structure(execution_record)
            if structure:
                self._parse(structure)

        # Process executor_result — same logic as old _execute_step result processing
        if isinstance(executor_result, Exception):
            self._finalize_step(step, execution_record, ExecutionStatus.FAILED,
                                error=f"Celery task error: {repr(executor_result)}")
            return

        actual_result = None
        error = None

        if isinstance(executor_result, dict):
            actual_result = executor_result.get('result')
            error = executor_result.get('error')
        else:
            actual_result = executor_result

        if error:
            self._finalize_step(step, execution_record, ExecutionStatus.FAILED, error=error)
            return

        if isinstance(actual_result, dict) and actual_result.get('error'):
            self._finalize_step(step, execution_record, ExecutionStatus.FAILED,
                                error=actual_result.get('error'), result=actual_result)
            return

        if isinstance(actual_result, dict) and actual_result.get('status') == ExecutionStatus.WAITING_ON_TASK:
            step_status = ExecutionStatus.WAITING_ON_TASK
        else:
            step_status = ExecutionStatus.COMPLETED

        step.status = step_status
        step.result = actual_result
        step.execution_end_date = datetime.utcnow()
        db.session.commit()

        if step_status == ExecutionStatus.WAITING_ON_TASK:
            execution_record.execution_status = ExecutionStatus.WAITING_ON_TASK
            execution_record.current_node_id = node_id
            db.session.commit()
            return

        # Update context with step result
        current_node = self._nodes.get(node_id, {})
        self._update_context(
            node_id=node_id,
            step_result=actual_result,
            sf_type=step.task_name or '',
            context=context
        )
        execution_record.input_data = context
        db.session.commit()

        # Determine and dispatch the next node(s)
        self._dispatch_next_nodes(execution_record, node_id, context)

    def _dispatch_next_nodes(self, execution_record: DefProcessExecution, from_node_id: str, context: dict) -> None:
        """Walk to and dispatch the next node(s) after an async step completes.

        Called from _complete_step_and_advance. Handles Stop (end),
        gateway (decision), and linear flows. Synchronously processes
        pass-through nodes and dispatches async tasks — the same logic
        as the main loop in execute_from_id.
        """
        current_node = self._nodes.get(from_node_id)
        if not current_node:
            return

        shape_name = current_node.get('data', {}).get('type', '')
        behavior = self._get_behavior(shape_name)
        user_id = execution_record.created_by

        # Stop node — end execution
        if behavior == NodeBehavior.EVENT and shape_name == 'Stop':
            execution_record.execution_status = ExecutionStatus.COMPLETED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.output_data = context
            db.session.commit()
            return

        # Gateway — evaluate decision
        if behavior == NodeBehavior.GATEWAY:
            try:
                target_id = self._evaluate_decision(current_node, context)
                target_node = self._nodes.get(target_id)
                if not target_node:
                    execution_record.execution_status = ExecutionStatus.FAILED
                    execution_record.error_message = f"Gateway target node '{target_id}' not found"
                    execution_record.execution_end_date = datetime.utcnow()
                    db.session.commit()
                    return
                next_nodes = [target_node]
            except Exception as e:
                execution_record.execution_status = ExecutionStatus.FAILED
                execution_record.error_message = f"Gateway evaluation error: {str(e)}"
                execution_record.execution_end_date = datetime.utcnow()
                db.session.commit()
                return
        else:
            # Linear flow
            next_nodes = self._get_next_nodes(from_node_id)
            if len(next_nodes) > 1:
                logger.warning(f"Node '{from_node_id}' has {len(next_nodes)} outgoing edges — only first will be followed")

        if not next_nodes:
            execution_record.execution_status = ExecutionStatus.COMPLETED
            execution_record.execution_end_date = datetime.utcnow()
            execution_record.output_data = context
            db.session.commit()
            return

        # Process the next node
        node = next_nodes[0]
        node_id = node['id']

        step_record = DefProcessExecutionStep(
            def_process_execution_id=execution_record.def_process_execution_id,
            node_id=node_id,
            node_label=node.get('data', {}).get('label', node_id),
            task_name=node.get('data', {}).get('step_function', ''),
            status=ExecutionStatus.RUNNING,
            input_data=context.copy(),
            created_by=user_id,
            last_updated_by=user_id
        )
        db.session.add(step_record)
        db.session.commit()

        result = self._execute_step_async(node, context)
        status = result.get('status')

        # Pass-through — process synchronously and chain
        if status in ('passed', 'skipped'):
            step_record.status = status
            step_record.execution_end_date = datetime.utcnow()
            db.session.commit()
            self._dispatch_next_nodes(execution_record, node_id, context)
            return

        # DISPATCHED — store task_id, chain continues via callback
        if status == 'DISPATCHED':
            step_record.celery_task_id = result.get('celery_task_id')
            step_record.status = 'DISPATCHED'
            if 'input_data' in result:
                step_record.input_data = result.get('input_data')
            db.session.commit()
            return

        # FAILED
        step_record.status = ExecutionStatus.FAILED
        step_record.error_message = result.get('error')
        if 'input_data' in result:
            step_record.input_data = result.get('input_data')
        step_record.execution_end_date = datetime.utcnow()
        db.session.commit()

        execution_record.execution_status = ExecutionStatus.FAILED
        execution_record.execution_end_date = datetime.utcnow()
        execution_record.error_message = result.get('error')
        db.session.commit()

    def _load_structure(self, execution_record: DefProcessExecution) -> Optional[dict]:
        """Retrieve the process graph JSON from the execution record or its DefProcess.

        Priority: execution_record.process_structure ->
        DefProcess.process_structure -> None.
        """
        if execution_record.process_structure:
            return execution_record.process_structure
        if execution_record.process_id:
            process = db.session.get(DefProcess, execution_record.process_id)
            if process:
                return process.process_structure
        return None

    def _finalize_step(self, step: DefProcessExecutionStep, execution: DefProcessExecution,
                       status: str, error: str = None, result: Any = None) -> None:
        """Set step and execution record to a terminal status (FAILED/COMPLETED).

        Flushes the error message, result, and end timestamp to both
        the step and execution rows in the database.
        """
        step.status = status
        step.error_message = error
        step.result = result
        step.execution_end_date = datetime.utcnow()
        db.session.commit()

        execution.execution_status = status
        execution.error_message = error
        execution.execution_end_date = datetime.utcnow()
        db.session.commit()

    def _evaluate_decision(self, node: dict, context: dict) -> str:
        """Evaluate a GATEWAY node and return the ID of the matching target node.

        Reads {node_id}_result (or fallback predictable_result) from
        context and compares it against each outgoing edge's lookup_value.
        Raises WorkflowError if no edge matches.
        """
        node_id = node['id']
        edges = self._edges_by_source.get(node_id, [])
        label = node.get('data', {}).get('label', node.get('id'))

        if not edges:
            raise WorkflowError(f"Decision node '{label}' has no outgoing edges")

        # Read from node-scoped key first, fall back to global predictable_result
        actual_val = str(
            context.get(f"{node_id}_result") or context.get('predictable_result', '')
        ).strip().lower()
        logger.debug(f"Decision '{label}': {node_id}_result={actual_val!r}")

        for edge in edges:
            edge_data = edge.get('data') or {}
            lookup_val = str(edge_data.get('lookup_value', '')).strip().lower()
            if lookup_val and lookup_val == actual_val:
                return edge['target']

        raise WorkflowError(f"Decision '{label}': no matching edge for '{actual_val}'")

    def run(self, process_id: int, context: dict = None, user_id: int = None,
            on_task_complete: Callable[[dict], None] = None) -> dict:
        """Convenience wrapper: initialize and execute a workflow synchronously.

        Creates the execution record, dispatches steps, and returns
        the final execution record as JSON. Legacy API — prefer
        initialize_execution + execute_from_id for async usage.
        """
        execution_id = self.initialize_execution(process_id, context, user_id)
        self.execute_from_id(execution_id, on_task_complete)
        execution = db.session.get(DefProcessExecution, execution_id)
        return execution.json()

    def resume_execution(self, execution_id: int, task_result: dict, on_task_complete: Callable[[dict], None] = None) -> None:
        """Resume a WAITING_ON_TASK execution with a result from an external source.

        Merges the provided task_result into the execution context,
        then calls execute_from_id to continue graph traversal from
        the saved current_node_id.
        """
        execution_record = db.session.get(DefProcessExecution, execution_id)
        if not execution_record:
            raise WorkflowError(f"Execution record not found: {execution_id}")

        if execution_record.execution_status != ExecutionStatus.WAITING_ON_TASK:
            raise WorkflowError(f"Execution is not in WAITING_ON_TASK state: {execution_record.execution_status}")

        node_id = execution_record.current_node_id
        if not node_id:
            raise WorkflowError("No current_node_id found on execution record to resume from")

        # Update context
        context = dict(execution_record.input_data or {})
        self._update_context(node_id, task_result, '', context)
        execution_record.input_data = context
        db.session.commit()

        # Resume engine execution — dispatches the next step
        self.execute_from_id(execution_id, on_task_complete=on_task_complete)

    def validate(self, process_structure: dict) -> List[str]:
        """Validate a process graph structure for correctness.

        Parses the nodes/edges and checks that a Start node exists.
        Returns a list of error messages (empty = valid).
        """
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
    """Module-level shortcut: instantiate a WorkflowEngine and call run()."""
    engine = WorkflowEngine()
    return engine.run(process_id, context, user_id, on_task_complete)
