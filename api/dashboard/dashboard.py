import time
from concurrent.futures import ThreadPoolExecutor
from flask import jsonify, current_app
from flask_jwt_extended import jwt_required
from executors.extensions import db, cache
from executors.models import (
    DefProcess,
    DefAsyncTaskSchedule,
    DefAsyncExecutionMethods,
    DefUser,
    DefTenant,
    DefAsyncTask,
    DefTenantEnterpriseSetup
)
from . import dashboard_bp


def _fetch_counts(app):
    with app.app_context():          # ← real app instance, not proxy
        return db.session.execute(db.text("""
            SELECT
                (SELECT COUNT(*) FROM apps.def_processes)                                   AS workflows,
                (SELECT COUNT(*) FROM def_async_tasks)                                      AS tasks_total,
                (SELECT COUNT(*) FROM def_async_tasks WHERE cancelled_yn = 'N')             AS tasks_active,
                (SELECT COUNT(*) FROM def_async_tasks WHERE cancelled_yn = 'Y')             AS tasks_inactive,
                (SELECT COUNT(*) FROM def_async_tasks WHERE srs = 'Y')                      AS tasks_srs,
                (SELECT COUNT(*) FROM def_async_tasks WHERE sf = 'Y')                       AS tasks_sf,
                (SELECT COUNT(*) FROM def_async_task_schedules WHERE schedule_type != 'IMMEDIATE' OR schedule_type IS NULL)                             AS schedules_total,
                (SELECT COUNT(*) FROM def_async_task_schedules WHERE cancelled_yn = 'N' AND (schedule_type != 'IMMEDIATE' OR schedule_type IS NULL))    AS schedules_scheduled,
                (SELECT COUNT(*) FROM def_async_task_schedules WHERE cancelled_yn = 'Y' AND (schedule_type != 'IMMEDIATE' OR schedule_type IS NULL))    AS schedules_cancelled,
                (SELECT COUNT(*) FROM def_async_execution_methods)                          AS executors,
                (SELECT COUNT(*) FROM apps.def_users)                                       AS users,
                (SELECT COUNT(*) FROM apps.def_tenants)                                     AS tenants,
                (SELECT COUNT(*) FROM apps.def_tenant_enterprise_setup)                     AS enterprises
        """)).first()


def _fetch_all_items(app):
    with app.app_context():
        rows = db.session.execute(db.text("""
            SELECT * FROM (
                SELECT 'workflow'   AS section, CAST(process_id AS VARCHAR)                AS id, process_name       AS name, creation_date 
                FROM apps.def_processes 
                ORDER BY creation_date DESC LIMIT 4
            ) w
            UNION ALL
            SELECT * FROM (
                SELECT 'async_task' AS section, CAST(def_task_id AS VARCHAR)               AS id, user_task_name     AS name, creation_date 
                FROM def_async_tasks 
                ORDER BY creation_date DESC LIMIT 4
            ) at
            UNION ALL
            SELECT * FROM (
                SELECT 'schedule'   AS section, CAST(def_task_sche_id AS VARCHAR)          AS id, user_schedule_name AS name, creation_date 
                FROM def_async_task_schedules 
                WHERE cancelled_yn = 'N' AND (schedule_type != 'IMMEDIATE' OR schedule_type IS NULL)
                ORDER BY creation_date DESC LIMIT 4
            ) s
            UNION ALL
            SELECT * FROM (
                SELECT 'executor'   AS section, CAST(internal_execution_method AS VARCHAR) AS id, execution_method   AS name, creation_date 
                FROM def_async_execution_methods 
                ORDER BY creation_date DESC LIMIT 4
            ) e
            UNION ALL
            SELECT * FROM (
                SELECT 'user'       AS section, CAST(user_id AS VARCHAR)                   AS id, user_name          AS name, creation_date 
                FROM apps.def_users 
                ORDER BY creation_date DESC LIMIT 4
            ) u
            UNION ALL
            SELECT * FROM (
                SELECT 'tenant'     AS section, CAST(tenant_id AS VARCHAR)                 AS id, tenant_name        AS name, creation_date 
                FROM apps.def_tenants 
                ORDER BY creation_date DESC LIMIT 4
            ) t
            UNION ALL
            SELECT * FROM (
                SELECT 'enterprise' AS section, CAST(tenant_id AS VARCHAR)                 AS id, enterprise_name    AS name, creation_date 
                FROM apps.def_tenant_enterprise_setup 
                ORDER BY creation_date DESC LIMIT 4
            ) en
        """)).fetchall()

        result = {k: [] for k in ['workflow', 'async_task', 'schedule', 'executor', 'user', 'tenant', 'enterprise']}
        for row in rows:
            result[row.section].append({
                "id": row.id,
                "name": row.name,
                "creation_date": row.creation_date
            })
        return result


@dashboard_bp.route('/dashboard/summary', methods=['GET'])
@jwt_required()
@cache.cached(timeout=60, key_prefix='dashboard_summary')
def get_dashboard_summary():
    try:
        # Capture the real app instance BEFORE entering threads
        app = current_app._get_current_object()  # ← key line

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_counts = executor.submit(_fetch_counts, app)   # pass app
            future_items  = executor.submit(_fetch_all_items, app) # pass app

            counts = future_counts.result()
            items  = future_items.result()

        return jsonify({
            "async_tasks": {
                "total":     counts.tasks_total,
                "active":    counts.tasks_active,
                "cancelled": counts.tasks_inactive,
                "srs":       counts.tasks_srs,
                "sf":        counts.tasks_sf,
                "items":     items['async_task']
            },
            "scheduled_tasks": {
                "total":     counts.schedules_total,
                "scheduled": counts.schedules_scheduled,
                "cancelled": counts.schedules_cancelled,
                "items":     items['schedule']
            },
            "executors":   {"total": counts.executors,   "items": items['executor']},
            "users":       {"total": counts.users,        "items": items['user']},
            "tenants":     {"total": counts.tenants,      "items": items['tenant']},
            "enterprises": {"total": counts.enterprises,  "items": items['enterprise']},
            "workflows":   {"total": counts.workflows,    "items": items['workflow']}
        }), 200

    except Exception as e:
        return jsonify({"message": "Error fetching dashboard summary", "error": str(e)}), 500