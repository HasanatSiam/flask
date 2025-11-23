# red_functions.py
from redbeat import RedBeatSchedulerEntry 
from celery import current_app as celery  
from datetime import timedelta 
from celery.schedules import schedule as celery_schedule

import logging

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_redbeat_schedule_old(schedule_name, executor, schedule_minutes=1, args=None, kwargs=None, celery_app=None):
    # Default values for args and kwargs
    args = args or []
    kwargs = kwargs or {}

    # Define the schedule interval
    schedule = timedelta(minutes=schedule_minutes)

    #args.extend([schedule_minutes, schedule_name])

    # Save the entry
    try:
        # Create the RedBeat entry
        entry = RedBeatSchedulerEntry(
        name=schedule_name,
        task=executor,
        schedule=schedule,
        args=args,
        kwargs=kwargs,
        app=celery_app
        )
        entry.save()
        print(f"RedBeat entry created: {entry.name}")
    except Exception as e:
        print(f"Failed to create RedBeat entry: {e}")
        raise

    return {"message": "Task scheduled successfully!", "entry_name": entry.name}


def create_redbeat_schedule(schedule_name, executor, schedule_minutes=None, cron_schedule=None, args=None, kwargs=None, celery_app=None):
    # Decide whether to use crontab (cron_schedule) or timedelta (schedule_minutes)
    if cron_schedule:
        schedule = cron_schedule  # Cron-based schedule

    elif schedule_minutes:
        schedule = timedelta(minutes=schedule_minutes)  # Time-based schedule
        
    else:
        raise ValueError("Neither cron_schedule nor schedule_minutes provided")
    
    args = args or []
    kwargs = kwargs or {}

    # Save the entry
    try:
        # Create the RedBeat entry
        entry = RedBeatSchedulerEntry(
        name=schedule_name,
        task=executor,
        schedule=schedule,
        args=args,
        kwargs=kwargs,
        app=celery_app
        )
        entry.save()
        print(f"RedBeat entry created: {entry.name}")
    except Exception as e:
        print(f"Failed to create RedBeat entry: {e}")

        raise

    return {"message": "Task scheduled successfully!", "entry_name": entry.name}

    args = args or []
    kwargs = kwargs or {}

    # Define the schedule interval
    schedule_seconds = schedule_minutes * 60

    try:
        # Fetch the existing RedBeat entry
        entry = RedBeatSchedulerEntry.from_key(f"redbeat:{schedule_name}", app=celery_app)
        # Validate if the task_name matches
        if entry.task != task:
            raise ValueError(f"Task name mismatch: Expected {task}, found {entry.task}")
        
        # Update entry fields
        entry.schedule = celery_schedule(schedule_seconds)  # Use celery.schedules.schedule for intervals
        entry.args = args  # Update args with extended values
        entry.kwargs = kwargs  # Update kwargs if provided

        # Save the updated entry back to Redis
        entry.save()
        print(f"RedBeat entry updated: {entry.name}")

    except Exception as e:
        print(f"Failed to update RedBeat entry: {e}")
        raise


def update_redbeat_schedule(schedule_name, task, schedule_minutes=None, cron_schedule=None, args=None, kwargs=None, celery_app=None):
   
    # Default values for args and kwargs
    args = args or []
    kwargs = kwargs or {}

    try:
        # Fetch the existing RedBeat entry
        entry = RedBeatSchedulerEntry.from_key(f"redbeat:{schedule_name}", app=celery_app)

        # Validate if the task_name matches
        if entry.task != task:
            raise ValueError(f"Task name mismatch: Expected '{task}', found '{entry.task}'")

        # Determine the correct schedule type
        if cron_schedule:
            entry.schedule = cron_schedule  # Use crontab scheduling
        elif schedule_minutes is not None:
            entry.schedule = celery_schedule(schedule_minutes * 60)  # Use interval scheduling
        else:
            raise ValueError("Either 'schedule_minutes' or 'cron_schedule' must be provided.")

        # Update entry fields
        entry.args = args
        entry.kwargs = kwargs

        # Save the updated entry back to Redis
        entry.save()
        print(f" RedBeat entry updated: {entry.name}")

    except Exception as e:
        print(f" Failed to update RedBeat entry: {e}")
        raise



def delete_schedule_from_redis(schedule_name):
    try:
        # Get Redis client from Celery's broker connection
        redis_client = celery.broker_connection().default_channel.client

        # Define the pattern for the RedBeat keys associated with the schedule name
        key_pattern = f"redbeat:{schedule_name}"

        # Retrieve all keys matching the pattern
        keys = redis_client.keys(pattern=key_pattern)

        if not keys:
            return {"message": f"Task '{schedule_name}' not found in Redis."}, 404

        # Attempt to delete each key
        for key in keys:
            redis_client.delete(key)

        return {"message": f"Task '{schedule_name}' deleted from Redis."}, 200

    except Exception as e:
        return {"error": f"Failed to delete schedule from Redis: {str(e)}"}, 500


