import json
import sys
import io
import os
from celery import shared_task

script_path = os.getenv("SCRIPT_PATH_01")

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    script_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None

    params = kwargs  # Use kwargs as script parameters
    full_script_path = os.path.join(script_path, script_name)

    try:
        # Capture script output
        original_stdout = sys.stdout
        sys.stdout = io.StringIO()

        # Read & Execute the script
        with open(full_script_path, 'r') as file:
            script_content = file.read()

        exec_globals = {"__builtins__": __builtins__}  # Safe execution context
        exec_globals.update(params)  # Inject parameters

        exec(script_content, exec_globals)

        # Get script output
        raw_output = sys.stdout.getvalue().strip()

        # Ensure output is a dictionary
        try:
            output = json.loads(raw_output)  # Convert JSON string to dictionary
        except json.JSONDecodeError:
            output = {"output": raw_output}  # Keep as dictionary if not JSON

        return {
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type":schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": params,
            "parameters": params,
            "result": output,  #  Ensures dictionary format
            "message": "Script executed successfully!"
        }

    except Exception as e:
        return {"error": f"Script execution failed: {str(e)}"}

    finally:
        # Restore stdout safely
        sys.stdout = original_stdout
