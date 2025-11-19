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

from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from utils.auth import role_required
from executors.models import DefUserCredential
from . import users_bp

@users_bp.route('/user_credentials/login', methods=['POST'])
def login(): ...



    
@flask_app.route('/def_user_credentials', methods=['POST'])
@jwt_required()
def create_user_credential():
    try:
        data = request.get_json()
        user_id = data['user_id']
        password = data['password']

        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)

        credential = DefUserCredential(
            user_id          = user_id,
            password         = hashed_password,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(credential)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully!"}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


    
    
@flask_app.route('/reset_user_password', methods=['PUT'])
@jwt_required()
def reset_user_password():
    try:
        data = request.get_json()
        current_user_id = data['user_id']
        old_password = data['old_password']
        new_password = data['new_password']

        user = DefUserCredential.query.get(current_user_id)
        if not user:
            return jsonify({'message': 'User not found'}), 404

        if not check_password_hash(user.password, old_password):
            return jsonify({'message': 'Invalid old password'}), 401

        hashed_new_password   = generate_password_hash(new_password, method='pbkdf2:sha256', salt_length=16)
        user.password         = hashed_new_password
        user.last_update_date = datetime.utcnow()
        user.last_updated_by  = get_jwt_identity()

        db.session.commit()

        return jsonify({'message': 'Edited successfully'}), 200

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@flask_app.route('/def_user_credentials/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user_credentials(user_id):
    try:
        credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if credential:
            db.session.delete(credential)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user credentials'}), 500)
  




@flask_app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        user = data.get('user', '').strip()
        password = data.get('password')

        if not user or not password:
            return jsonify({"message": "Email/Username and Password are required."}), 400

        user_record = DefUser.query.filter(
            (DefUser.email_address.ilike(f"%{user}%")) |
            (DefUser.user_name == user)
        ).first()

        access_profile = DefAccessProfile.query.filter(
            func.trim(DefAccessProfile.profile_id).ilike(f"%{user}%"),
            func.trim(DefAccessProfile.profile_type).ilike("Email")
        ).first()

        user_id = None
        if user_record:
            user_id = user_record.user_id
        elif access_profile:
            user_id = access_profile.user_id

        if not user_id:
            return jsonify({"message": "User not found."}), 404

        user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
        if not user_cred:
            return jsonify({"message": "User credentials not found."}), 404

        if not check_password_hash(user_cred.password, password):
            return jsonify({"message": "Invalid email/username or password."}), 401


        access_token = create_access_token(identity=str(user_id))

        return jsonify({
            "isLoggedIn": True,
            "user_id": user_id,
            "access_token": access_token
        }), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500


