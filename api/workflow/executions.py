import json
import time
import logging

from flask import request, jsonify, Response, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from executors.extensions import db
from executors.models import DefProcessExecution, DefProcessExecutionStep
from . import workflow_bp

@workflow_bp.route('/workflow/executions', methods=['GET'])
@jwt_required()
def get_workflow_executions():
    try:
        process_id = request.args.get('process_id')
        def_process_execution_id = request.args.get('def_process_execution_id')
        
        if def_process_execution_id:
            execution = DefProcessExecution.query.get(def_process_execution_id)
            if not execution:
                return jsonify({"error": "Execution not found"}), 404
            # Return as a list for consistency with this endpoint's format
            return jsonify({"result": [execution.json()]}), 200

        if not process_id:
             return jsonify({"error": "Missing process_id query parameter"}), 400
             
        executions = DefProcessExecution.query.filter_by(process_id=process_id)\
            .order_by(DefProcessExecution.execution_start_date.desc()).all()
            
        return jsonify({"result": [e.json() for e in executions]}), 200
        
    except Exception as e:
        return jsonify({"message": "Error fetching executions", "error": str(e)}), 500


@workflow_bp.route('/workflow/execution_steps', methods=['GET'])
@jwt_required()
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
def stream_execution(execution_id):
    """
    SSE endpoint for real-time workflow execution status.
    
    Streams events:
    - event: 'step'      — individual step status update (new step or status changed)
    - event: 'complete'   — final execution result when workflow finishes
    - event: 'heartbeat'  — periodic keep-alive (every ~15s)
    - event: 'error'      — error notification
    - event: 'timeout'    — stream exceeded max duration
    
    Supports SSE reconnection via Last-Event-ID header.
    """
    current_user = get_jwt_identity()
    if not current_user:
        return jsonify({"error": "Authentication required"}), 401
    
    app = current_app._get_current_object()
    
    def _sse(event, data, event_id=None):
        """Format and encode a single SSE message."""
        parts = []
        if event_id is not None:
            parts.append(f"id: {event_id}")
        parts.append(f"event: {event}")
        parts.append(f"data: {json.dumps(data)}")
        return ("\n".join(parts) + "\n\n").encode('utf-8')
    
    def generate():
        with app.app_context():
            last_step_states = {}        # {step_id: status}
            event_counter = 0            # SSE event ID for reconnection
            max_wait_seconds = 3600      # 1 hour max stream duration
            consecutive_errors = 0       # Track DB/query errors
            max_consecutive_errors = 5   # Give up after 5 consecutive failures
            start_time = time.time()
            last_heartbeat = 0           # Throttle heartbeats
            
            try:
                while True:
                    elapsed = time.time() - start_time
                    if elapsed > max_wait_seconds:
                        yield _sse('timeout', {'message': 'Stream timeout'})
                        break
                    
                    try:
                        # Rollback closes current transaction snapshot so the next
                        # query sees committed changes from other sessions/threads
                        db.session.rollback()
                        
                        execution = DefProcessExecution.query.get(execution_id)
                        if not execution:
                            yield _sse('error', {'message': 'Execution not found'})
                            break
                        
                        steps = DefProcessExecutionStep.query.filter_by(
                            def_process_execution_id=execution_id
                        ).order_by(DefProcessExecutionStep.execution_start_date.asc()).all()
                        
                        current_status = execution.execution_status
                        consecutive_errors = 0  # Reset on successful query
                            
                        # Emit only new or changed steps
                        for step in steps:
                            s_id = step.def_execution_step_id
                            s_status = step.status
                            
                            if s_id not in last_step_states or last_step_states[s_id] != s_status:
                                event_counter += 1
                                yield _sse('step', step.json(), event_counter)
                                last_step_states[s_id] = s_status

                        # Check completion
                        if current_status not in ['RUNNING', 'QUEUED']:
                            event_counter += 1
                            yield _sse('complete', execution.json(), event_counter)
                            break
                        
                        # Throttled heartbeat — every 5 seconds
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
                        continue  # Skip the normal polling sleep
                        
                    # Adaptive polling interval
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
