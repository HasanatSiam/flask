import json
import time
import logging

from flask import request, jsonify, Response, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from utils.auth import role_required
from executors.extensions import db
from executors.models import DefProcessExecution, DefProcessExecutionStep, DefProcess
from . import workflow_bp

@workflow_bp.route('/workflow/executions', methods=['GET'])
@jwt_required()
@role_required()
def get_workflow_executions():
    """
    GET /workflow/executions

    Query params:
      - def_process_execution_id  : fetch single execution by ID
      - process_id                : filter by process ID
      - process_name              : filter by process name (partial, case-insensitive)
      - page      (default 1)     : pagination page
      - limit     (default 20)    : results per page
    """
    try:
        process_id             = request.args.get('process_id')
        def_process_execution_id = request.args.get('def_process_execution_id')
        process_name           = request.args.get('process_name', '').strip()
        page                   = request.args.get('page',  1,  type=int)
        limit                  = request.args.get('limit', 20, type=int)

        # --- Single execution by ID ---
        if def_process_execution_id:
            execution = DefProcessExecution.query.get(def_process_execution_id)
            if not execution:
                return jsonify({"error": "Execution not found"}), 404
            return jsonify({"result": [execution.json()], "total": 1, "pages": 1, "page": 1}), 200

        # --- Build base query ---
        query = DefProcessExecution.query

        if process_id:
            query = query.filter(DefProcessExecution.process_id == process_id)

        if process_name:
            query = query.join(DefProcess, DefProcess.process_id == DefProcessExecution.process_id)\
                         .filter(DefProcess.process_name.ilike(f'%{process_name}%'))

        query = query.order_by(DefProcessExecution.execution_start_date.desc())

        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return jsonify({
            "result": [e.json() for e in paginated.items],
            "total":  paginated.total,
            "pages":  paginated.pages,
            "page":   paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error fetching executions", "error": str(e)}), 500


@workflow_bp.route('/workflow/execution_steps', methods=['GET'])
@jwt_required()
@role_required()
def get_workflow_execution_steps():
    try:
        def_process_execution_id = request.args.get('def_process_execution_id')
        node_id = request.args.get('node_id')
        
        if not def_process_execution_id:
            return jsonify({"error": "Missing def_process_execution_id query parameter"}), 400
            
        query = DefProcessExecutionStep.query.filter_by(def_process_execution_id=def_process_execution_id)
        
        if node_id:
            query = query.filter_by(node_id=node_id)
            
        steps = query.order_by(DefProcessExecutionStep.execution_start_date.asc()).all()
            
        if node_id and len(steps) == 1:
            return jsonify({"result": steps[0].json()}), 200

        return jsonify({"result": [s.json() for s in steps]}), 200
        
    except Exception as e:
        return jsonify({"message": "Error fetching execution steps", "error": str(e)}), 500


@workflow_bp.route('/workflow/execution_stream/<int:execution_id>', methods=['GET'])
@jwt_required(optional=True, locations=['headers', 'query_string'])
@role_required()
def stream_execution(execution_id):
    """
    SSE endpoint for real-time workflow execution status.

    Streams events:
    - event: 'step'      — individual step status update
    - event: 'complete'  — final execution result
    - event: 'heartbeat' — periodic keep-alive (every ~15s)
    - event: 'error'     — error notification
    - event: 'timeout'   — stream exceeded max duration
    """
    current_user = get_jwt_identity()
    if not current_user:
        return jsonify({"error": "Authentication required"}), 401

    app = current_app._get_current_object()

    def _sse(event, data, event_id=None):
        parts = []
        if event_id is not None:
            parts.append(f"id: {event_id}")
        parts.append(f"event: {event}")
        parts.append(f"data: {json.dumps(data)}")
        return ("\n".join(parts) + "\n\n").encode('utf-8')

    def generate():
        with app.app_context():
            last_step_states = {}
            event_counter = 0
            max_wait_seconds = 3600
            consecutive_errors = 0
            max_consecutive_errors = 5
            start_time = time.time()
            last_heartbeat = 0

            try:
                while True:
                    elapsed = time.time() - start_time
                    if elapsed > max_wait_seconds:
                        yield _sse('timeout', {'message': 'Stream timeout'})
                        break

                    try:
                        db.session.rollback()

                        execution = DefProcessExecution.query.get(execution_id)
                        if not execution:
                            yield _sse('error', {'message': 'Execution not found'})
                            break

                        steps = DefProcessExecutionStep.query.filter_by(
                            def_process_execution_id=execution_id
                        ).order_by(DefProcessExecutionStep.execution_start_date.asc()).all()

                        current_status = execution.execution_status
                        consecutive_errors = 0

                        for step in steps:
                            s_id = step.def_execution_step_id
                            s_status = step.status
                            if s_id not in last_step_states or last_step_states[s_id] != s_status:
                                event_counter += 1
                                yield _sse('step', step.json(), event_counter)
                                last_step_states[s_id] = s_status

                        if current_status not in ['RUNNING', 'WAITING_ON_TASK']:
                            event_counter += 1
                            yield _sse('complete', execution.json(), event_counter)
                            break

                        if elapsed - last_heartbeat >= 5:
                            event_counter += 1
                            yield _sse('heartbeat', {'status': current_status}, event_counter)
                            last_heartbeat = elapsed

                    except Exception as e:
                        consecutive_errors += 1
                        app.logger.error(f"Stream error for execution {execution_id}: {e}")
                        yield _sse('error', {'message': f'Stream error: {str(e)}'})
                        if consecutive_errors >= max_consecutive_errors:
                            yield _sse('error', {'message': 'Too many consecutive errors, closing stream'})
                            break
                        time.sleep(2)
                        continue

                    if elapsed < 60:
                        time.sleep(1.0)
                    elif elapsed < 300:
                        time.sleep(2.0)
                    else:
                        time.sleep(5.0)

            except GeneratorExit:
                pass
            except Exception as outer_e:
                app.logger.error(f"Stream generation fatal error: {outer_e}")
                try:
                    yield _sse('error', {'message': 'Internal stream error'})
                except Exception:
                    pass
            finally:
                db.session.close()

    return Response(
        generate(),
        mimetype='text/event-stream',
        direct_passthrough=True,
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@workflow_bp.route('/workflow/execution/live/<int:execution_id>', methods=['GET'])
@jwt_required()
# @role_required()
def poll_execution_live(execution_id):
    """
    Poll endpoint for live workflow execution status.

    Client polls every ~2s. The response tells you:
      - What's running RIGHT NOW (current_step_id)
      - All steps so far (diff by def_execution_step_id)
      - Whether the workflow is done

    Query params:
      - node_id  (optional) — filter steps to return only this node's step

    Response:
    {
      "execution_id":    445,
      "status":          "RUNNING" | "WAITING_ON_TASK" | "COMPLETED" | "FAILED",
      "current_step_id": 5,                        // the step currently RUNNING/DISPATCHED
      "steps":           [ ... all steps ... ],    // filtered by node_id if provided
      "done":            false
    }
    """
    try:
        node_id = request.args.get('node_id')

        execution = DefProcessExecution.query.get(execution_id)
        if not execution:
            return jsonify({"error": "Execution not found"}), 404

        steps_query = DefProcessExecutionStep.query.filter_by(
            def_process_execution_id=execution_id
        )

        if node_id:
            steps_query = steps_query.filter_by(node_id=node_id)

        steps = steps_query.order_by(
            DefProcessExecutionStep.execution_start_date.asc()
        ).all()

        # Derive current_step_id from steps array — no second DB query
        current_step_id = None
        for s in steps:
            if s.status in ('RUNNING', 'DISPATCHED'):
                current_step_id = s.def_execution_step_id

        return jsonify({
            "execution_id":     execution_id,
            "status":           execution.execution_status,
            "current_node_id":  execution.current_node_id,
            "current_step_id":  current_step_id,
            "steps":            [s.json() for s in steps],
            "done":             execution.execution_status not in ['RUNNING', 'WAITING_ON_TASK']
        }), 200

    except Exception as e:
        return jsonify({"message": "Error polling execution", "error": str(e)}), 500
