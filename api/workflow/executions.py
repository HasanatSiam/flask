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
    - type: 'step' - specific step update (running, completed, failed)
    - type: 'status' - Overall execution status update
    - type: 'complete' - Final result when workflow finishes
    """
    # Verify we have a valid identity
    current_user = get_jwt_identity()
    if not current_user:
        return jsonify({"error": "Authentication required"}), 401
    
    # Capture the app for use inside the generator
    app = current_app._get_current_object()
    
    def generate():
        with app.app_context():
            # Track state of steps we've seen: {step_id: status}
            last_step_states = {}
            max_wait_seconds = 3600  # 1 hour timeout
            start_time = time.time()
            logger = logging.getLogger(__name__)
            
            try:
                while True:
                    # Calculate elapsed time
                    elapsed = time.time() - start_time
                    if elapsed > max_wait_seconds:
                        yield f"event: timeout\ndata: {json.dumps({'message': 'Stream timeout'})}\n\n"
                        break
                    
                    try:
                        # Refresh DB state - use updated query usage pattern
                        # We must commit/rollback to close the current transaction and see changes 
                        # from other threads/processes (especially with Repeatable Read isolation)
                        db.session.commit()
                        
                        execution = DefProcessExecution.query.get(execution_id)
                        if not execution:
                            yield f"event: error\ndata: {json.dumps({'message': 'Execution not found'})}\n\n"
                            break
                        
                        # Get all steps
                        steps = DefProcessExecutionStep.query.filter_by(
                            def_process_execution_id=execution_id
                        ).order_by(DefProcessExecutionStep.execution_start_date.asc()).all()
                        
                        current_status = execution.execution_status
                    
                    # Log heartbeat details to debug stuck workflows
                        if int(elapsed) % 10 == 0:  # Log every ~10 seconds
                            logger.info(f"Stream {execution_id}: Status={current_status}, Steps={len(steps)}")
                            
                        # Check for step updates
                        for step in steps:
                            s_id = step.def_execution_step_id
                            s_status = step.status
                            
                            # Emit if new step or status changed
                            if s_id not in last_step_states or last_step_states[s_id] != s_status:
                                # Send full step data
                                yield f"event: step\ndata: {json.dumps(step.json())}\n\n"
                                last_step_states[s_id] = s_status

                        # Check completion
                        if current_status not in ['RUNNING', 'QUEUED']:
                            yield f"event: complete\ndata: {json.dumps(execution.json())}\n\n"
                            break
                        
                        # Heartbeat
                        yield f"event: heartbeat\ndata: {json.dumps({'status': current_status})}\n\n"
                        
                    except Exception as e:
                        yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
                        time.sleep(1)
                        
                    # Polling interval
                    if elapsed < 60:
                        time.sleep(0.5)
                    elif elapsed < 300:
                        time.sleep(2.0)
                    else:
                        time.sleep(5.0)

            except GeneratorExit:
                # Client disconnected
                pass
            except Exception as outer_e:
                logger.error(f"Stream generation error: {outer_e}")
                yield f"event: error\ndata: {json.dumps({'message': 'Internal stream error'})}\n\n"
            finally:
                db.session.close()

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )
