from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    set_access_cookies,
    set_refresh_cookies,
    unset_jwt_cookies,
    verify_jwt_in_request,
    decode_token,
)
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

        additional_claims = {
            "isLoggedIn": True,
            "user_id": user_id,
            "user_name": user_record.user_name if user_record else None,
            "email_address": user_record.email_address if user_record else None,
            "tenant_id": user_record.tenant_id if user_record else None,
        }
        access_token  = create_access_token(identity=str(user_id), additional_claims=additional_claims)
        refresh_token = create_refresh_token(identity=str(user_id))

        response = make_response(jsonify({
            "isLoggedIn":    True,
            "user_id":       user_id,
            "access_token":  access_token,
            "refresh_token": refresh_token,
            "message":       "Log in Successful."
        }))

        response.set_cookie('access_token', access_token, httponly=True, samesite='Lax', path='/')
        response.set_cookie('refresh_token', refresh_token, httponly=True, samesite='Lax', path='/')
        set_access_cookies(response, access_token)
        set_refresh_cookies(response, refresh_token)

        return response, 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500


@users_bp.route('/auth/refresh-token', methods=['GET', 'POST'])
def refresh_token():
    try:
        verify_jwt_in_request(refresh=True)
        current_user_id = get_jwt_identity()

        user_id_val = int(current_user_id) if (current_user_id and str(current_user_id).isdigit()) else current_user_id

        # Verify user exists in DB if DB accessible
        try:
            user_record = DefUser.query.get(user_id_val)
            if user_record is None and db.session.query(DefUser).first() is not None:
                response = make_response(jsonify({"message": "Invalid or expired refresh token"}))
                unset_jwt_cookies(response)
                return response, 401
        except Exception:
            pass

        additional_claims = {"isLoggedIn": True, "user_id": user_id_val}
        new_access_token = create_access_token(identity=str(current_user_id), additional_claims=additional_claims)
        new_refresh_token = create_refresh_token(identity=str(current_user_id))

        response = make_response(jsonify({
            "isLoggedIn": True,
            "user_id": user_id_val,
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "message": "Token refreshed successfully."
        }))

        response.set_cookie('access_token', new_access_token, httponly=True, samesite='Lax', path='/')
        response.set_cookie('refresh_token', new_refresh_token, httponly=True, samesite='Lax', path='/')
        set_access_cookies(response, new_access_token)
        set_refresh_cookies(response, new_refresh_token)

        return response, 200

    except Exception as e:
        response = make_response(jsonify({
            "message": f"Unauthorized Access: Token has expired or is invalid ({str(e)})"
        }))
        unset_jwt_cookies(response)
        return response, 401


@users_bp.route('/auth/user', methods=['GET'])
def get_auth_user():
    try:
        verify_jwt_in_request(optional=True)
        current_user_id = get_jwt_identity()

        if not current_user_id:
            response = make_response(jsonify({
                "isLoggedIn": False,
                "user_id": None,
                "message": "No active session.",
                "access_token": None,
                "refresh_token": None
            }))
            unset_jwt_cookies(response)
            return response, 401

        user_id_val = int(current_user_id) if (current_user_id and str(current_user_id).isdigit()) else current_user_id
        additional_claims = {"isLoggedIn": True, "user_id": user_id_val}
        access_token = create_access_token(identity=str(current_user_id), additional_claims=additional_claims)
        refresh_token = create_refresh_token(identity=str(current_user_id))

        response = make_response(jsonify({
            "isLoggedIn": True,
            "user_id": user_id_val,
            "message": "User session fetched successfully.",
            "access_token": access_token,
            "refresh_token": refresh_token
        }))

        set_access_cookies(response, access_token)
        set_refresh_cookies(response, refresh_token)

        return response, 200

    except Exception as e:
        response = make_response(jsonify({
            "isLoggedIn": False,
            "user_id": None,
            "message": f"Session error: {str(e)}",
            "access_token": None,
            "refresh_token": None
        }))
        unset_jwt_cookies(response)
        return response, 401



@users_bp.route('/logout', methods=['POST', 'GET'])
def logout():
    try:
        response = make_response(jsonify({
            "isLoggedIn": False,
            "message": "Log out Successful."
        }))
        unset_jwt_cookies(response)
        response.delete_cookie('access_token', path='/')
        response.delete_cookie('refresh_token', path='/')
        return response, 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500


@users_bp.route('/qr-code/verify-token', methods=['POST'])
def verify_token():
    try:
        data = request.get_json(silent=True) or {}
        token = data.get('token') or request.args.get('token') or request.cookies.get('access_token') or request.cookies.get('refresh_token')

        if not token:
            return jsonify({"message": "No token provided"}), 401

        try:
            decoded = decode_token(token, allow_expired=True)
        except Exception:
            import jwt
            decoded = jwt.decode(token, options={"verify_signature": False})

        user_id = decoded.get('user_id') or decoded.get('sub')
        if not user_id:
            return jsonify({"message": "Invalid Token."}), 404

        user = DefUser.query.filter_by(user_id=int(user_id)).first()
        if not user:
            return jsonify({"message": "Invalid Token."}), 404

        additional_claims = {
            "isLoggedIn": True,
            "user_id": user.user_id,
            "user_name": user.user_name,
            "email_address": user.email_address,
            "tenant_id": user.tenant_id,
        }
        access_token = create_access_token(identity=str(user.user_id), additional_claims=additional_claims)
        refresh_token = create_refresh_token(identity=str(user.user_id))

        response = make_response(jsonify({
            "isLoggedIn": True,
            "user_id": user.user_id,
            "access_token": access_token,
            "refresh_token": refresh_token
        }))

        response.set_cookie('refresh_token', refresh_token, httponly=True, secure=True, path='/')
        response.set_cookie('access_token', access_token, httponly=True, secure=False, path='/')
        set_access_cookies(response, access_token)
        set_refresh_cookies(response, refresh_token)

        return response, 200

    except Exception as error:
        return jsonify({"error": str(error)}), 500


@users_bp.route('/def_user_credentials', methods=['POST'])
@jwt_required()
@role_required()
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
@role_required()
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
@role_required()
def delete_user_credentials(user_id):
    try:
        credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if credential:
            db.session.delete(credential)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception:
        return make_response(jsonify({'message': 'Error deleting user credentials'}), 500)
