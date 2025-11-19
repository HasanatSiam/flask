import subprocess
import os
import json
from celery import shared_task

script_path = os.getenv("SCRIPT_PATH_02")  # Base directory for scripts

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    script_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None

    full_script_path = os.path.join(script_path, script_name)

    # Ensure the script exists
    if not os.path.exists(full_script_path):
        return {"error": f"Script '{script_name}' not found at '{script_path}'"}

    # Ensure the script is executable
    if not os.access(full_script_path, os.X_OK):
        return {"error": f"Permission denied: '{full_script_path}' is not executable"}

    try:
        # Execute the shell script and capture output
        result = subprocess.run(
            [full_script_path] + [str(arg) for arg in args[6:]],  # Pass additional arguments to the script
            text=True,
            capture_output=True,
            check=True
        )





        # Try parsing the output as JSON
        try:
            output_json = json.loads(result.stdout.strip())
        except json.JSONDecodeError:
            output_json = {"output": result.stdout.strip()}  # Fallback if not valid JSON

        return {
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type":schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": kwargs,
            "parameters": kwargs,
            "result": output_json,  # Return parsed JSON output
            "message": "Shell script executed successfully!"
        }

    except subprocess.CalledProcessError as e:
        return {
            "error": f"Shell script execution failed: {e.returncode}",
            "stderr": e.stderr.strip() if e.stderr else "No error output",
            "stdout": e.stdout.strip() if e.stdout else "No output"
        }

    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
