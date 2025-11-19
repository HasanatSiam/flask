import ast
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required
from datetime import datetime
from datetime import datetime, timedelta
import requests
from sqlalchemy import or_
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAsyncTaskRequest

)
from config import FLOWER_URL
from . import async_task_bp

# flower_url = flask_app.config["FLOWER_URL"]

@async_task_bp.route('/view_requests_v1', methods=['GET'])
@jwt_required()
def get_all_tasks():
    try:
        fourteen_days = datetime.utcnow() - timedelta(days=2)
        tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= fourteen_days).order_by(DefAsyncTaskRequest.creation_date.desc())
        #tasks = DefAsyncTaskRequest.query.limit(100000).all()
        if not tasks:
            return jsonify({"message": "No tasks found"}), 404
        return jsonify([task.json() for task in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@async_task_bp.route('/view_requests_v2', methods=['GET'])
@jwt_required()
def view_requests_v2():
    try:
        fourteen_days = datetime.utcnow() - timedelta(days=4)
        tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= fourteen_days).order_by(DefAsyncTaskRequest.creation_date.desc())
        #tasks = DefAsyncTaskRequest.query.limit(100000).all()
        if not tasks:
            return jsonify({"message": "No tasks found"}), 404
        return jsonify([task.json() for task in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500





#def_async_task_requests
@async_task_bp.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
@jwt_required()
def view_requests(page, page_limit):
    try:
        # Query params
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        # Case 1: task_name provided but days not provided -> default days = 30
        if search_query and days is None:
            days = 7

        # Apply days filter if available (now days will be set in all needed cases)
        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        # Apply task_name search if provided (with TRIM to avoid space issues)
        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))


        # if search_query:
        #     normalized_search = search_query.replace('_', ' ')
        #     query = query.filter(or_(
        #         func.lower(func.replace(func.trim(DefAsyncTaskRequest.task_name), '_', ' ')).ilike(f'%{normalized_search}%')
        #     ))

        # Order newest first
        query = query.order_by(DefAsyncTaskRequest.creation_date.desc())

        # Paginate results
        paginated = query.paginate(page=page, per_page=page_limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No tasks found"}), 404

        return jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@async_task_bp.route('/view_requests/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_task_requests_view_requests(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        day_limit = datetime.utcnow() - timedelta(days=30)
        query = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= day_limit)

        if search_query:
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [req.json() for req in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching view requests", "error": str(e)}), 500)



@async_task_bp.route('/view_requests_v3/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def combined_tasks_v3(page, limit):
    try:
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        if search_query and days is None:
            days = 7

        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        db_tasks = paginated.items

        flower_tasks = {}
        try:
            res = requests.get(f"{FLOWER_URL}/api/tasks", timeout=5)
            if res.status_code == 200:
                flower_tasks = res.json()
        except:
            pass  # keep flower_tasks empty if error

        items = []

        
        for t in db_tasks:
            item = t.json()  # get all DB fields
            # add Flower fields
            if t.task_id and t.task_id in flower_tasks:
                ftask = flower_tasks[t.task_id]
                item["uuid"] = ftask.get("uuid")
                item["state"] = ftask.get("state")
                item["worker"] = ftask.get("worker")
            else:
                item["uuid"] = None
                item["state"] = None
                item["worker"] = None
            items.append(item)

        return make_response(jsonify({
            "items": items,
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@async_task_bp.route('/view_requests_v4/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def combined_tasks_v4(page, limit):
    try:
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        if search_query and days is None:
            days = 7

        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        db_tasks = paginated.items

        # db_tasks = query.order_by(DefAsyncTaskRequest.creation_date.desc()).all()
        db_task_ids = {t.task_id for t in db_tasks if t.task_id}

        flower_tasks = {}
        try:
            res = requests.get(f"{FLOWER_URL}/api/tasks", timeout=5)
            if res.status_code == 200:
                flower_tasks = res.json()
        except:
            pass  # keep flower_tasks empty if error

        items = []


        # Add Flower tasks first (skip duplicates if already in DB)
        for tid, ftask in flower_tasks.items():
            # if tid in db_task_ids:
            #     continue  # skip duplicate
            if tid in db_task_ids or ftask.get("state", "").lower() == "success":
                continue 

            args_list = []
            try:
                args_list = ast.literal_eval(ftask.get("args", "[]"))  # safely parse string repr
            except Exception:
                args_list = []

            script_name          = args_list[0] if len(args_list) > 0 else None
            user_task_name       = args_list[1] if len(args_list) > 1 else None
            task_name            = args_list[2] if len(args_list) > 2 else None
            user_schedule_name   = args_list[3] if len(args_list) > 3 else None
            redbeat_schedule_name= args_list[4] if len(args_list) > 4 else None
            schedule_type        = args_list[5] if len(args_list) > 5 else None
            schedule_data        = args_list[6] if len(args_list) > 6 else None

            items.append({
                "uuid": ftask.get("uuid"),
                "task_id": tid,
                "script_name": script_name,
                "user_task_name": user_task_name,
                "task_name": task_name,                   
                "user_schedule_name": user_schedule_name,
                "redbeat_schedule_name": redbeat_schedule_name,
                "schedule_type": schedule_type,
                "schedule": schedule_data,
                "state": ftask.get("state"),
                "worker": ftask.get("worker"),
                # "timestamp": ftask.get("timestamp"),
                "result": ftask.get("result"),
                "args": ftask.get("args"),                 
                "kwargs": ftask.get("kwargs"),
                "parameters": None,                        # DB-only field
                "request_id": None,
                "executor": None,
                "created_by": None,
                "creation_date": None,
                "last_updated_by": None,
                "last_updated_date": None
            })

        
        for t in db_tasks:
            item = t.json()  # get all DB fields
            # add Flower fields
            if t.task_id and t.task_id in flower_tasks:
                ftask = flower_tasks[t.task_id]
                item["uuid"] = ftask.get("uuid")
                item["state"] = ftask.get("state")
                item["worker"] = ftask.get("worker")
            else:
                item["uuid"] = None
                item["state"] = None
                item["worker"] = None
            items.append(item)

        
        

        return make_response(jsonify({
            "items": items,
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    

        # --- Manual pagination on merged list ---
        # total = len(items)
        # start = (page - 1) * limit
        # end = start + limit
        # paginated_items = items[start:end]
        # pages = (total + limit - 1) // limit  # ceil division


        # return make_response(jsonify({
        #     "items": paginated_items,
        #     "total": total,
        #     "pages": pages,
        #     "page": page
        # }), 200)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

