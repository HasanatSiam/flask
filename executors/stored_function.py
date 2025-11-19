import os
import psycopg2
from celery import shared_task
import logging
import json

logging.basicConfig(level=logging.INFO)

db_url = os.getenv("DATABASE_URL")

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    stored_function_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None
    params = kwargs

    conn = None
    cursor = None

    try:
        logging.info("Attempting to connect to the database for function")
        conn = psycopg2.connect(db_url)
        logging.info("Database connection for function successful.")

        if conn is None:
            logging.error("Database connection object for function is None.")
            return {"error": "Failed to establish database connection for function."}

        cursor = conn.cursor()

        if params:
            placeholders = ', '.join(['%s'] * len(params))
            call_query = f"SELECT {stored_function_name}({placeholders});"
            cursor.execute(call_query, list(params.values()))
        else:
            call_query = f"SELECT {stored_function_name}();"
            cursor.execute(call_query)

        output = None
        if cursor.description:
            output = cursor.fetchall()
            if output:
                output = output[0][0]  
                logging.info(f"Stored function output: {output}")

        conn.commit()

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
            "result": output,  
            "message": "Stored function executed successfully!"
        }

    except psycopg2.Error as db_err:
        logging.error(f"psycopg2 error (function): {db_err}", exc_info=True)
        return {"error": f"Stored function execution failed: {str(db_err)}"}
    except Exception as e:
        logging.exception("An unexpected error occurred (function):")
        return {"error": f"Stored function execution failed: {str(e)}"}

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logging.error(f"Failed to close cursor (function): {e}", exc_info=True)

        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logging.error(f"Failed to close connection (function): {e}", exc_info=True)
