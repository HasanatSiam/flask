import requests
from celery import shared_task

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    script_name = args[0] if len(args) > 0 else None  # Will be None for HTTP executor
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None

    url = kwargs.pop('url', None)
    if not url:
        return {"error": "Missing 'url' in parameters."}

    method = kwargs.pop('method', None)
    if not method:
        return {"error": "Missing HTTP 'method'. Please specify 'method' in parameters (e.g., GET, POST, PUT, DELETE)."}
    method = method.upper()

    headers = kwargs.pop('headers', None)
    # Set default headers only for POST and PUT if headers not provided
    if not headers and method in ['POST', 'PUT']:
        headers = {'Content-Type': 'application/json'}

    payload = kwargs  # Remaining kwargs are used as request body or query params

    # Log the request info for debugging
    # print(f"Executing HTTP task: {method} {url}")
    # print(f"Headers: {headers}")
    # print(f"Payload: {payload}")

    try:
        response = None

        if method == 'GET':
            if payload:
                response = requests.get(url, headers=headers, params=payload)
            else:
                response = requests.get(url, headers=headers)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=payload)
        elif method == 'PUT':
            response = requests.put(url, headers=headers, json=payload)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, json=payload)
        else:
            return {"error": f"Unsupported HTTP method: {method}"}

        # Try to decode JSON, fallback to raw text
        try:
            result_data = response.json()
        except ValueError:
            result_data = {"response_text": response.text}

        # print("result_data:", result_data)

        return {
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type": schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": payload,
            "url": url,
            "method": method,
            "status_code": response.status_code,
            "result": result_data,
            "message": "HTTP request executed successfully."
        }

    except Exception as e:
        return {"error": f"HTTP request execution failed: {str(e)}"}
