import os 
import time
import re
import json
import uuid
import requests
import traceback
import logging
import time
import re
import ast
import math
from redis import Redis
from requests.auth import HTTPBasicAuth
from zoneinfo import ZoneInfo
from itertools import count
from functools import wraps
from flask_cors import CORS 
from dotenv import load_dotenv            # To load environment variables from a .env file
from celery.schedules import crontab
from celery.result import AsyncResult      # For checking the status of tasks
from redbeat import RedBeatSchedulerEntry
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Text, desc, cast, TIMESTAMP, func, or_, text
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses
from itsdangerous import BadSignature,SignatureExpired, URLSafeTimedSerializer
from flask_mail import Message as MailMessage
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, decode_token
from werkzeug.security import generate_password_hash, check_password_hash
from executors import flask_app # Import Flask app and tasks
from executors.extensions import db
from celery import current_app as celery  # Access the current Celery app
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam,
    DefAsyncTaskSchedule,
    DefAsyncTaskRequest,
    DefAsyncTaskSchedulesV,
    DefAsyncExecutionMethods,
    DefAsyncTaskScheduleNew,
    DefTenant,
    DefUser,
    DefPerson,
    DefUserCredential,
    DefAccessProfile,
    DefUsersView,
    Message,
    DefTenantEnterpriseSetup,
    DefTenantEnterpriseSetupV,
    DefAccessModel,
    DefAccessModelLogic,
    DefAccessModelLogicAttribute,
    DefGlobalCondition,
    DefGlobalConditionLogic,
    DefGlobalConditionLogicAttribute,
    DefAccessPoint,
    DefAccessPointsV,
    DefDataSource,
    DefAccessEntitlement,
    DefControl,
    DefActionItem,
    DefActionItemsV,
    DefActionItemAssignment,
    DefAlert,
    DefAlertRecipient,
    DefProcess,
    DefControlEnvironment,
    NewUserInvitation,
    DefJobTitle,
    DefAccessEntitlementElement,
    DefNotifications,
    DefRoles,
    DefUserGrantedRole,
    DefPrivilege,
    DefUserGrantedPrivilege,
    DefApiEndpoint,
    DefApiEndpointRole
)
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1
from config import redis_url

from flask import Blueprint, jsonify, make_response, request
from flask_jwt_extended import jwt_required
from sqlalchemy import or_
from executors.models import DefUsersView
from utils.auth import role_required

combined_user_bp = Blueprint("combined_user_bp", __name__)

@combined_user_bp.route("/def_combined_user/<int:page>/<int:limit>", methods=["GET"])
@jwt_required()
@role_required()
def get_paginated_combined_users(page, limit):
    pass  # real function omitted as requested



@flask_app.route('/def_combined_user/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_combined_users(page, limit):
    try:
        query = DefUsersView.query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching users', 'error': str(e)}), 500)
        

@flask_app.route('/def_combined_user/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_combined_users(page, limit):
    user_name = request.args.get('user_name', '').strip()
    try:
        query = DefUsersView.query
        if user_name:
            query = query.filter(DefUsersView.user_name.ilike(f'%{user_name}%'))
        query = query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching users', 'error': str(e)}), 500)

