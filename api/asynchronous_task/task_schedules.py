
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
import uuid
import logging
from celery.schedules import crontab     
from sqlalchemy import or_
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_mail import Message as MailMessage
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from celery import current_app as celery  # Access the current Celery app
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task_v1


from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam,
    DefAsyncTaskSchedule,
    DefAsyncTaskScheduleNew,
    DefAsyncTaskSchedulesV

)
from . import async_task_bp


@async_task_bp.route('/Create_TaskSchedule', methods=['POST'])
@jwt_required()
def Create_TaskSchedule():
    try:
        user_schedule_name = request.json.get('user_schedule_name', 'Immediate')
        task_name = request.json.get('task_name')
        parameters = request.json.get('parameters', {})
        schedule_type = request.json.get('schedule_type')
        schedule_data = request.json.get('schedule', {})

        if not task_name:
            return jsonify({'error': 'Task name is required'}), 400

        # Fetch task details from the database
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not task:
            return jsonify({'error': f'No task found with task_name: {task_name}'}), 400

        # Prevent scheduling if the task is cancelled
        if getattr(task, 'cancelled_yn', 'N') == 'Y':
            return jsonify({'error': f"Task '{task_name}' is cancelled and cannot be scheduled."}), 400

        user_task_name = task.user_task_name
        executor = task.executor
        script_name = task.script_name

        schedule_name = str(uuid.uuid4())
        # redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
        redbeat_schedule_name = None
        if schedule_type != "IMMEDIATE":
            redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"

        args = [script_name, user_task_name, task_name, user_schedule_name, redbeat_schedule_name, schedule_type, schedule_data]
        kwargs = {}

        # Validate task parameters
        task_params = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()
        for param in task_params:
            param_name = param.parameter_name
            if param_name in parameters:
                kwargs[param_name] = parameters[param_name]
            else:
                return jsonify({'error': f'Missing value for parameter: {param_name}'}), 400

        # Handle scheduling based on schedule type
        cron_schedule = None
        schedule_minutes = None

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])  # e.g., ["Monday", "Wednesday"]
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])  # e.g., ["5", "15"]
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')  # e.g., {"date": "2025-03-01 14:30"}
            if not one_time_date:
                return jsonify({'error': 'Date is required for one-time execution'}), 400
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            # Extract frequency type and frequency value from schedule_data
            frequency_type_raw = schedule_data.get('FREQUENCY_TYPE', 'MINUTES')
            frequency_type = frequency_type_raw.upper().strip().rstrip('s').replace('(', '').replace(')', '')
            frequency = schedule_data.get('FREQUENCY', 1)

            # Log frequency values to help with debugging
            print(f"Frequency Type: {frequency_type}")
            print(f"Frequency: {frequency}")
           
            # Handle different frequency types
            if frequency_type == 'MONTHS':
               schedule_minutes = frequency * 30 * 24 * 60  # Approximate calculation: 1 month = 30 days
            elif frequency_type == 'WEEKS':
               schedule_minutes = frequency * 7 * 24 * 60  # 7 days * 24 hours * 60 minutes
            elif frequency_type == 'DAYS':
               schedule_minutes = frequency * 24 * 60  # 1 day = 24 hours = 1440 minutes
            elif frequency_type == 'HOURS':
               schedule_minutes = frequency * 60  # 1 hour = 60 minutes
            elif frequency_type == 'MINUTES':
               schedule_minutes = frequency  # Frequency is already in minutes
            else:
               return jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400

        # elif schedule_type == "MONTHLY_LAST_DAY":

        #     try:
        #         today = datetime.today()
        #         start_year = today.year
        #         start_month = today.month

        #         # Calculate how many months left in the year including current month
        #         months_left = 12 - start_month + 1  # +1 to include the current month itself

        #         for i in range(months_left):
        #             # Calculate the target year and month
        #             year = start_year  # same year, no spanning next year
        #             month = start_month + i

        #             # Find the first day of the next month
        #             if month == 12:
        #                 next_month = datetime(year + 1, 1, 1)
        #             else:
        #                 next_month = datetime(year, month + 1, 1)

        #             # Calculate the last day of the current month
        #             last_day_dt = next_month - timedelta(days=1)
        #             last_day = last_day_dt.day

        #             # Create a cron schedule for the last day of this month at midnight
        #             cron_schedule = crontab(
        #                 minute=0,
        #                 hour=0,
        #                 day_of_month=last_day,
        #                 month_of_year=month
        #             )

        #             redbeat_schedule_name = f"{user_schedule_name}_{uuid.uuid4()}"

        #             args_with_schedule = [
        #                 script_name,
        #                 user_task_name,
        #                 task_name,
        #                 user_schedule_name,
        #                 redbeat_schedule_name,
        #                 schedule_type,
        #                 schedule_data
        #             ]

        #             # Create Redis schedule entry via RedBeat
        #             create_redbeat_schedule(
        #                 schedule_name=redbeat_schedule_name,
        #                 executor=executor,
        #                 cron_schedule=cron_schedule,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 celery_app=celery
        #             )

        #             # Create database record for the schedule
        #             new_schedule = DefAsyncTaskScheduleNew(
        #                 user_schedule_name=user_schedule_name,
        #                 redbeat_schedule_name=redbeat_schedule_name,
        #                 task_name=task_name,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 parameters=kwargs,
        #                 schedule_type=schedule_type,
        #                 schedule={"scheduled_for": f"{year}-{month:02}-{last_day} 00:00"},
        #                 cancelled_yn='N',
        #                 created_by=101
        #             )

        #             db.session.add(new_schedule)

        #         db.session.commit()

        #         return jsonify({
        #             "message": f"Monthly last-day tasks scheduled for the remaining {months_left} months of {start_year}"
        #         }), 201

        #     except Exception as e:
        #         db.session.rollback()
        #         return jsonify({
        #             "error": "Failed to schedule monthly last-day tasks",
        #             "details": str(e)
        #         }), 500


        
        # Handle Ad-hoc Requests
        elif schedule_type == "IMMEDIATE":
            try:
                result = execute_ad_hoc_task_v1(
                    user_schedule_name = user_schedule_name,
                    executor = executor,
                    task_name = task_name,
                    args = args,
                    kwargs = kwargs,
                    schedule_type = schedule_type,
                    cancelled_yn = 'N',
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                return jsonify(result), 201
            except Exception as e:
                return jsonify({"error": "Failed to execute ad-hoc task", "details": str(e)}), 500

        else:
            return jsonify({'error': 'Invalid schedule type'}), 400
        # Handle Scheduled Tasks
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor,
                schedule_minutes=schedule_minutes if schedule_minutes else None,
                cron_schedule=cron_schedule if cron_schedule else None,
                args=args,
                kwargs=kwargs,
                celery_app=celery
            )
        except Exception as e:
            return jsonify({"error": "Failed to create RedBeat schedule", "details": str(e)}), 500

        # if schedule_type != "IMMEDIATE":
        # Store schedule in DB
        new_schedule = DefAsyncTaskScheduleNew(
            user_schedule_name = user_schedule_name,
            redbeat_schedule_name = redbeat_schedule_name,
            task_name = task_name,
            args = args,
            kwargs = kwargs,
            parameters = kwargs,
            schedule_type = schedule_type,
            schedule = schedule_data,
            cancelled_yn = 'N',
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_schedule)
        db.session.commit()

        # return jsonify({
        #     "message": "Task schedule created successfully!",
        #     "schedule_id": new_schedule.def_task_sche_id
        # }), 201
        return jsonify({"message": "Added successfully"}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to add task schedule", "details": str(e)}), 500


@async_task_bp.route('/Show_TaskSchedules', methods=['GET'])
@jwt_required()
def Show_TaskSchedules():
    try:
    #     schedules = DefAsyncTaskSchedulesV.query \
    # .filter(DefAsyncTaskSchedulesV.ready_for_redbeat != 'Y') \
    # .order_by(desc(DefAsyncTaskSchedulesV.def_task_sche_id)) \
    # .all()
        schedules = DefAsyncTaskSchedulesV.query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).all()
        # Return the schedules as a JSON response
        return jsonify([schedule.json() for schedule in schedules])

    except Exception as e:
        # Handle any errors and return them as a JSON response
        return jsonify({"error": str(e)}), 500



@async_task_bp.route('/def_async_task_schedules/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_task_schedules(page, limit):
    try:
        paginated = DefAsyncTaskSchedulesV.query.order_by(
            DefAsyncTaskSchedulesV.def_task_sche_id.desc()
        ).paginate(page=page, per_page=limit, error_out=False)

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error fetching task schedules", "error": str(e)}), 500


@async_task_bp.route('/def_async_task_schedules/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_task_schedules(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTaskSchedulesV.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_query}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_underscore}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error searching task schedules", "error": str(e)}), 500





@async_task_bp.route('/Show_TaskSchedule/<string:task_name>', methods=['GET'])
@jwt_required()
def Show_TaskSchedule(task_name):
    try:
        schedule = DefAsyncTaskSchedule.query.filter_by(task_name=task_name).first()
        if schedule:
            return make_response(jsonify(schedule.json()), 200)

        return make_response(jsonify({"message": f"Task Periodic Schedule for {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Task Periodic Schedule", "error": str(e)}), 500)





@async_task_bp.route('/Update_TaskSchedule/<string:task_name>', methods=['PUT'])
@jwt_required()
def Update_TaskSchedule(task_name):
    try:
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400

        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name, redbeat_schedule_name=redbeat_schedule_name
        ).first()
        executors = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not schedule:
            return jsonify({"message": f"Task Periodic Schedule for {redbeat_schedule_name} not found"}), 404

        # if schedule.ready_for_redbeat != 'N':
        #     return jsonify({
        #         "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
        #     }), 400

        # Update fields
        schedule.parameters = request.json.get('parameters', schedule.parameters)
        schedule.kwargs = request.json.get('parameters', schedule.kwargs)
        schedule.schedule_type = request.json.get('schedule_type', schedule.schedule_type)
        schedule.schedule = request.json.get('schedule', schedule.schedule)
        schedule.last_updated_by = get_jwt_identity()
        schedule.last_update_date = datetime.utcnow()

        # Handle scheduling logic
        cron_schedule = None
        schedule_minutes = None

        if schedule.schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule.schedule.get('VALUES', [])
            day_map = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule.schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule.schedule.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule.schedule_type == "ONCE":
            one_time_date = schedule.schedule.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule.schedule_type == "PERIODIC":
            frequency_type = schedule.schedule.get('FREQUENCY_TYPE', 'minutes').lower()
            frequency = schedule.schedule.get('FREQUENCY', 1)
            
            if frequency_type == 'months':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'days':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'hours':
                schedule_minutes = frequency * 60
            else:
                schedule_minutes = frequency  # Default to minutes

        # Ensure at least one scheduling method is provided
        if not schedule_minutes and not cron_schedule:
            return jsonify({"message": "Either 'schedule_minutes' or 'cron_schedule' must be provided."}), 400

        # Update RedBeat schedule
        try:
            update_redbeat_schedule(
                schedule_name = redbeat_schedule_name,
                task = executors.executor,
                schedule_minutes = schedule_minutes,
                cron_schedule = cron_schedule,
                args = schedule.args,
                kwargs = schedule.kwargs,
                celery_app = celery
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"message": "Error updating Redis. Database changes rolled back.", "error": str(e)}), 500

        db.session.commit()
        # return jsonify({"message": f"Task Schedule for {redbeat_schedule_name} updated successfully in database and Redis"}), 200
        return jsonify({"message": "Edited successfully"}), 200


    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error editing Task Schedule", "error": str(e)}), 500


@async_task_bp.route('/Cancel_TaskSchedule/<string:task_name>', methods=['PUT'])
@jwt_required()
def Cancel_TaskSchedule(task_name):
    try:
        # Extract redbeat_schedule_name from payload
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400)

        # Find the task schedule in the database
        schedule = DefAsyncTaskScheduleNew.query.filter_by(task_name=task_name, redbeat_schedule_name=redbeat_schedule_name).first()

        if not schedule:
            return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} not found"}), 404)

        # Check if ready_for_redbeat is 'N' (only then cancellation is allowed)
        # if schedule.ready_for_redbeat != 'N':
        #     return make_response(jsonify({"message": f"Cancellation not allowed. Task periodic schedule for {redbeat_schedule_name} is already processed in Redis"}), 400)

        # Update the `cancelled_yn` field to 'Y' (marking it as cancelled)
        schedule.cancelled_yn = 'Y'

        # Commit the change to the database
        db.session.commit()

        # Now, call the function to delete the schedule from Redis
        redis_response, redis_status = delete_schedule_from_redis(redbeat_schedule_name)

        # If there is an issue deleting from Redis, rollback the database update
        if redis_status != 200:
            db.session.rollback()
            return make_response(jsonify({"message": "Task schedule cancelled, but failed to delete from Redis", "error": redis_response['error']}), 500)

        # Return success message if both operations are successful
        # return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} has been cancelled successfully in the database and deleted from Redis"}), 200)
        return make_response(jsonify({"message": "Cancelled successfully"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on failure
        return make_response(jsonify({"message": "Error cancelling task periodic schedule", "error": str(e)}), 500)


@async_task_bp.route('/Reschedule_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Reschedule_TaskSchedule(task_name):
    try:
        data = request.get_json()
        redbeat_schedule_name = data.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({'error': 'redbeat_schedule_name is required'}), 400)

        # Find the cancelled schedule in DB
        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name,
            redbeat_schedule_name=redbeat_schedule_name,
            cancelled_yn='Y'
        ).first()

        if not schedule:
            return make_response(jsonify({'error': 'Cancelled schedule not found'}), 404)

        # Determine cron or periodic schedule
        cron_schedule = None
        schedule_minutes = None
        schedule_data = schedule.schedule
        schedule_type = schedule.schedule_type

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            frequency_type_raw = schedule_data.get('FREQUENCY_TYPE', 'MINUTES')
            frequency_type = frequency_type_raw.upper().strip().rstrip('s').replace('(', '').replace(')', '')
            frequency = schedule_data.get('FREQUENCY', 1)

            if frequency_type == 'MONTHS':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'WEEKS':
                schedule_minutes = frequency * 7 * 24 * 60
            elif frequency_type == 'DAYS':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'HOURS':
                schedule_minutes = frequency * 60
            elif frequency_type == 'MINUTES':
                schedule_minutes = frequency
            else:
                return make_response(jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400)

        else:
            return make_response(jsonify({'error': f'Cannot reschedule type: {schedule_type}'}), 400)
        
        executor = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not executor:
            return make_response(jsonify({'error': f'Executor not found for task {task_name}'}),404)
        # Restore schedule in Redis
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor.executor,     
                schedule_minutes=schedule_minutes,
                cron_schedule=cron_schedule,
                args=schedule.args,
                kwargs=schedule.kwargs,
                celery_app=celery
            )
            print(executor)
            
        except Exception as e:
            return make_response(jsonify({'error': 'Failed to recreate RedBeat schedule', 'details': str(e)}), 500)

        # Update DB
        schedule.cancelled_yn = 'N'
        schedule.last_updated_by = get_jwt_identity()
        schedule.last_update_date = datetime.utcnow()
        db.session.commit()

        # return make_response(jsonify({'message': f"Schedule '{redbeat_schedule_name}' has been rescheduled."}), 200)
        return make_response(jsonify({'message': "Rescheduled Successfully."}), 200)


    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': 'Failed to reschedule task', 'details': str(e)}), 500)





@async_task_bp.route('/Cancel_AdHoc_Task/<string:task_name>/<string:user_schedule_name>/<string:schedule_id>/<string:task_id>', methods=['PUT'])
def Cancel_AdHoc_Task(task_name, user_schedule_name, schedule_id, task_id):
    """
    Cancels an ad-hoc task by updating the database and revoking the Celery task.

    Args:
        task_name (str): The name of the Celery task.
        user_schedule_name (str): The name of the user schedule.
        schedule_id (str): The database schedule ID.
        task_id (str): The Celery task ID.

    Returns:
        JSON response indicating success or failure.
    """
    try:
        # Find the task schedule by schedule_id and user_schedule_name
        schedule = DefAsyncTaskSchedule.query.filter_by(
            def_task_sche_id=schedule_id,
            user_schedule_name=user_schedule_name,
            task_name=task_name
        ).first()

        if schedule:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            schedule.cancelled_yn = 'Y'

            # Commit the change to the database
            db.session.commit()

            # Now, revoke the Celery task
            try:
                celery.control.revoke(task_id, terminate=True)
                logging.info(f"Ad-hoc task with ID '{task_id}' revoked successfully.")
            except Exception as e:
                db.session.rollback()
                return make_response(jsonify({
                    "message": "Task schedule cancelled, but failed to revoke Celery task.",
                    "error": str(e)
                }), 500)

            # Return success message if both operations are successful
            return make_response(jsonify({
                "message": f"Ad-hoc task for schedule_id {schedule_id} has been successfully cancelled and revoked."
            }), 200)

        # If no schedule was found
        return make_response(jsonify({
            "message": f"No ad-hoc task found for schedule_id {schedule_id} and user_schedule_name {user_schedule_name}."
        }), 404)

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error cancelling ad-hoc task: {str(e)}")
        return make_response(jsonify({
            "message": "Error cancelling ad-hoc task.",
            "error": str(e)
        }), 500)

    finally:
        db.session.close()

