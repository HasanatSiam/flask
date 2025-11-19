# ad_hoc_functions.py

from celery import current_app as celery  # Access the current Celery app

from celery.schedules import schedule as celery_schedule

import logging
from executors.extensions import db 
from executors.models import DefAsyncTaskSchedule, DefAsyncTaskScheduleNew


def execute_ad_hoc_task(user_schedule_name, task_name, executor, args, kwargs, cancelled_yn, created_by, creation_date=None, last_updated_by=None, last_update_date=None):
    """
    Executes a task immediately using Celery and logs the execution in the database.

    Args:
        task_name (str): The name of the task to execute.
        args (list): The positional arguments for the task.
        kwargs (dict): The keyword arguments for the task.
        cancelled_yn (str): The cancellation status ('N' by default).
        created_by (int): ID of the user who created the task.

    Returns:
        dict: A success response containing task execution details.

    Raises:
        Exception: If the task execution or database operation fails.
    """
    try:
        # Validate argument types
        if not isinstance(args, list):
            raise ValueError("`args` must be a list.")
        if not isinstance(kwargs, dict):
            raise ValueError("`kwargs` must be a dictionary.")

        # Execute the task immediately using Celery
        celery.send_task(executor, args=args, kwargs=kwargs)

        # Log the execution in the database
        new_schedule = DefAsyncTaskSchedule(
            user_schedule_name = user_schedule_name,
            task_name = task_name,
            args = args,
            kwargs = kwargs,
            cancelled_yn = cancelled_yn,
            created_by = created_by,
            creation_date = creation_date,
            last_updated_by = last_updated_by,
            last_update_date = last_update_date
        )
        db.session.add(new_schedule)
        db.session.commit()

        logging.info(f"Ad-hoc task executed and logged successfully.")

        # Return a success response
        return {
            "message": "Ad-hoc task executed and logged successfully!",
            "schedule_id": new_schedule.def_task_sche_id
        }
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error executing ad-hoc task: {task_name}. Details: {str(e)}")
        raise Exception(f"Failed to execute and log ad-hoc task: {str(e)}")
    finally:
        db.session.close()




def execute_ad_hoc_task_v1(user_schedule_name, task_name, executor, args, kwargs, schedule_type, cancelled_yn, created_by, creation_date=None, last_updated_by=None, last_update_date=None):
    """
    Executes a task immediately using Celery and logs the execution in the database.

    Args:
        task_name (str): The name of the task to execute.
        args (list): The positional arguments for the task.
        kwargs (dict): The keyword arguments for the task.
        cancelled_yn (str): The cancellation status ('N' by default).
        created_by (int): ID of the user who created the task.

    Returns:
        dict: A success response containing task execution details.

    Raises:
        Exception: If the task execution or database operation fails.
    """
    try:
        # Validate argument types
        if not isinstance(args, list):
            raise ValueError("`args` must be a list.")
        if not isinstance(kwargs, dict):
            raise ValueError("`kwargs` must be a dictionary.")

        # Execute the task immediately using Celery
        celery.send_task(executor, args=args, kwargs=kwargs)

        # Log the execution in the database
        new_schedule = DefAsyncTaskScheduleNew(
            user_schedule_name = user_schedule_name,
            task_name = task_name,
            args = args,
            kwargs = kwargs,
            parameters = kwargs,
            schedule_type = schedule_type,
            cancelled_yn = cancelled_yn,
            created_by = created_by,
            creation_date = creation_date,
            last_updated_by = last_updated_by,
            last_update_date = last_update_date
        )
        db.session.add(new_schedule)
        db.session.commit()

        logging.info(f"Ad-hoc task executed and logged successfully.")

        # Return a success response
        return {
            "message": "Ad-hoc task executed and logged successfully!",
            "schedule_id": new_schedule.def_task_sche_id
        }
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error executing ad-hoc task: {task_name}. Details: {str(e)}")
        raise Exception(f"Failed to execute and log ad-hoc task: {str(e)}")
    finally:
        db.session.close()
