import os
import psycopg2
from celery import shared_task
import logging

logging.basicConfig(level=logging.INFO)

db_url = os.getenv("DATABASE_URL")

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    stored_procedure_name = args[0] if len(args) > 0 else None
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
        logging.info("Attempting to connect to the database...")
        conn = psycopg2.connect(db_url)
        logging.info("Database connection successful.")

        if conn is None:
            logging.error("Database connection object is None.")
            return {"error": "Failed to establish database connection."}

        cursor = conn.cursor()

        # Add a placeholder for the OUT parameter
        # call_query = f"CALL {stored_procedure_name}(%s);"
        # out_param = None  # This will hold the output value

        # cursor.execute(call_query, (out_param,))  # Execute the stored procedure
        # output = cursor.fetchone()  # Fetch the output

        param_values = list(params.values()) if params else []
        placeholders = ', '.join(['%s'] * len(param_values))
        if placeholders:
            call_query = f"CALL {stored_procedure_name}({placeholders});"
            cursor.execute(call_query, param_values)
        else:
            call_query = f"CALL {stored_procedure_name}();"
            cursor.execute(call_query)

        # If your procedure returns something, fetch it here (optional)
        output = None
        try:
            output = cursor.fetchone()
        except Exception:
            output = None

        conn.commit()

        return {
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
            "result": output[0] if output else None,  
            "message": "Stored procedure executed successfully!"
        }

    except psycopg2.Error as db_err:
        logging.error(f"psycopg2 error: {db_err}", exc_info=True)
        return {"error": f"Stored procedure execution failed: {str(db_err)}"}
    except Exception as e:
        logging.exception("An unexpected error occurred:")
        return {"error": f"Stored procedure execution failed: {str(e)}"}

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logging.error(f"Failed to close cursor: {e}", exc_info=True)

        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logging.error(f"Failed to close connection: {e}", exc_info=True)


# import os
# import psycopg2
# from celery import shared_task
# import logging
# import json

# logging.basicConfig(level=logging.INFO)

# db_url = os.getenv("database_url_01")

# @shared_task(bind=True)
# def execute(self, *args, **kwargs):
#     stored_procedure_name = args[0] if len(args) > 0 else None
#     user_task_name = args[1] if len(args) > 1 else None
#     task_name = args[2] if len(args) > 2 else None
#     user_schedule_name = args[3] if len(args) > 3 else None
#     redbeat_schedule_name = args[4] if len(args) > 4 else None
#     schedule = args[5] if len(args) > 5 else None
#     params = kwargs

#     conn = None
#     cursor = None

#     try:
#         logging.info("Attempting to connect to the database for procedure execution.")
#         conn = psycopg2.connect(db_url)
#         logging.info("Database connection for procedure successful.")

#         if conn is None:
#             logging.error("Database connection object for procedure is None.")
#             return {"error": "Failed to establish database connection for procedure."}

#         cursor = conn.cursor()

#         # Call the procedure with an OUT parameter using DO $$ block
#         cursor.execute("DO $$ DECLARE output_msg TEXT; BEGIN CALL proc_hello_world(output_msg); RETURN output_msg; END $$;")

#         # Fetch the OUT parameter result
#         cursor.execute("SELECT output_msg;")
#         output = cursor.fetchone()[0]  # Extract the OUT parameter value

#         logging.info(f"Stored procedure output: {output}")

#         conn.commit()

#         return {
#             "user_task_name": user_task_name,
#             "task_name": task_name,
#             "executor": self.name,
#             "user_schedule_name": user_schedule_name,
#             "redbeat_schedule_name": redbeat_schedule_name,
#             "schedule": schedule,
#             "args": args,
#             "kwargs": params,
#             "parameters": params,
#             "result": {"message": output},  # Return the result as JSON
#             "message": "Stored procedure executed successfully!"
#         }

#     except psycopg2.Error as db_err:
#         logging.error(f"psycopg2 error (procedure): {db_err}", exc_info=True)
#         return {"error": f"Stored procedure execution failed: {str(db_err)}"}

#     except Exception as e:
#         logging.exception("An unexpected error occurred (procedure):")
#         return {"error": f"Stored procedure execution failed: {str(e)}"}

#     finally:
#         if cursor is not None:
#             try:
#                 cursor.close()
#             except Exception as e:
#                 logging.error(f"Failed to close cursor (procedure): {e}", exc_info=True)

#         if conn is not None:
#             try:
#                 conn.close()
#             except Exception as e:
#                 logging.error(f"Failed to close connection (procedure): {e}", exc_info=True)
