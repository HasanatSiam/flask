import json
import sys
import io
import os
from celery import shared_task, states
from celery.exceptions import Ignore, CeleryError, Reject


script_path = os.getenv("SCRIPT_PATH_01")

# class TaskFailed(CeleryError):
#     pass

# CUSTOM_STATES = {"COMPLETED", "FAILED_EXECUTION", "SCRIPT_NOT_FOUND"}
# states.ALL_STATES = states.ALL_STATES.union(CUSTOM_STATES)
# states.READY_STATES = states.READY_STATES.union(CUSTOM_STATES)


@shared_task(bind=True)
def execute(self, *args, **kwargs):
    script_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None
    
    
    params = kwargs
    full_script_path = os.path.join(script_path, script_name) if script_name else None

    original_stdout = sys.stdout
    sys.stdout = io.StringIO()

    try:
        if not script_name or not os.path.exists(full_script_path):
            raise FileNotFoundError(f"Script not found: {full_script_path}")

        with open(full_script_path, 'r') as file:
            script_content = file.read()

        exec_globals = {"__builtins__": __builtins__}
        exec_globals.update(params)

        exec(script_content, exec_globals)

        raw_output = sys.stdout.getvalue().strip()

        try:
            output = json.loads(raw_output)
        except json.JSONDecodeError:
            output = {"output": raw_output}

        result_data = {
            "task_id": self.request.id,
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type": schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": params,
            "parameters": params,
            "result": output,
            "status": "SUCCESS",
            "message": "Script executed successfully"
        }

        return result_data  #stored in taskmeta.result on success

    except Exception as exc:
        # build same schema, mark failure
        result_data = {
            "task_id": self.request.id,
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type": schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": params,
            "parameters": params,
            "result": None,
            "status": "FAILURE",
            "message": str(exc),
            "exc_type": type(exc).__name__,
        }

        #store this directly into taskmeta.result
        self.update_state(
            # state=states.FAILURE,
            state='COMPLETED',
            meta=result_data
        )


        # return result_data

        #! DOESN"T STORE DATA BUT CELERY FLOWER STATE CHNAGES TO FAILURE

        # raise exc
        # raise Exception()
        # raise TaskFailed(str(exc))

        #! STORE DATA BUT CELERY FLOWER STATE REMAINS STARTED

        raise Ignore()
        # raise Reject(exc, requeue=False)

    finally:
        sys.stdout = original_stdout
        



