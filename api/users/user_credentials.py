from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token, create_refresh_token, set_access_cookies, set_refresh_cookies
from sqlalchemy import func
from datetime import datetime
from utils.auth import role_required
from executors.models import DefUserCredential, DefUser, DefAccessProfile
from executors.extensions import db
from . import users_bp


@users_bp.route('/login', methods=['POST'])
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

        additional_claims = {"isLoggedIn": True, "user_id": user_id}
        access_token  = create_access_token(identity=str(user_id), additional_claims=additional_claims)
        refresh_token = create_refresh_token(identity=str(user_id))

        response = make_response(jsonify({
            "isLoggedIn":    True,
            "user_id":       user_id,
            "access_token":  access_token,
            "refresh_token": refresh_token,
            "message":       "Log in Successful."
        }))

        set_access_cookies(response, access_token)
        set_refresh_cookies(response, refresh_token)

        return response, 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500


@users_bp.route('/def_user_credentials', methods=['POST'])
@jwt_required()
def create_user_credential():
    try:
        data    = request.get_json()
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


@users_bp.route('/reset_user_password', methods=['PUT'])
@jwt_required()
def reset_user_password():
    try:
        data             = request.get_json()
        current_user_id  = data['user_id']
        old_password     = data['old_password']
        new_password     = data['new_password']

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


@users_bp.route('/def_user_credentials/<int:user_id>', methods=['DELETE'])
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
