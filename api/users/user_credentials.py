from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token
from sqlalchemy import or_, func
from datetime import datetime, timedelta
from flask_mail import Message as MailMessage
from utils.auth import encrypt, decrypt, decode_token
import random
from config import crypto_secret_key, invitation_expire_time, mail
from utils.auth import role_required
from executors.models import(DefUserCredential, 
                             DefUser, 
                             DefAccessProfile,
                             DefUsersView,
                             ForgotPasswordRequest)
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


        access_token = create_access_token(identity=str(user_id))

        return jsonify({
            "isLoggedIn": True,
            "user_id": user_id,
            "access_token": access_token
        }), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500



    
@users_bp.route('/def_user_credentials', methods=['POST'])
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
       
    
@users_bp.route('/reset_user_password', methods=['PUT'])
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
  



@users_bp.route("/create_request", methods=["POST"])
def create_request():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Request body must be JSON."}), 400

        user_name = data.get("user_name")
        email = data.get("email_address")
        dob = data.get("date_of_birth")

        if not user_name or not email or not dob:
            return jsonify({"error": "Username, Email, and Date of Birth are required."}), 400

        try:
            dob_obj = datetime.strptime(dob, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "Date of Birth must be in YYYY-MM-DD format."}), 400

        user = DefUsersView.query.filter(
            db.func.lower(DefUsersView.user_name) == user_name.lower(),
            DefUsersView.email_address == email,
            DefUsersView.date_of_birth == dob_obj
        ).first()

        if not user:
            return jsonify({"message": "Input field is invalid"}), 400

        # ------------------ JWT ------------------
        token = create_access_token(
        identity=str(user.user_id),   
        expires_delta=timedelta(minutes=invitation_expire_time))
        encrypted_token = encrypt(token, crypto_secret_key)

        # ------------------ Create Forgot Password Request ------------------
        temp_password = str(random.randint(10000000, 99999999))
        req_obj = ForgotPasswordRequest(
            request_by=user.user_id,
            email=user.email_address,
            temporary_password=temp_password,
            access_token=encrypted_token,
            created_by=user.user_id,
            last_updated_by=user.user_id,
            is_valid=True
        )
        db.session.add(req_obj)
        db.session.commit()

        encrypted_req_id = encrypt(str(req_obj.request_id), crypto_secret_key)
        encrypted_user_id = encrypt(str(user.user_id), crypto_secret_key)

        reset_link = f"{request.host_url}reset-password/{encrypted_req_id}/{encrypted_user_id}/{encrypted_token}"

        # ------------------ Send Email ------------------
        try:
            msg = MailMessage(
                subject="You’re invited to reset your password",
                recipients=[email],
                # html=f"""
                #     <p>Hello,</p>
                #     <p>The temporary password is: {temp_password}</p>
                #     <p>Click the link below to reset your password:</p>
                #     <p><a href="{reset_link}">Reset Password</a></p>
                #     <p>Best regards,<br>PROCG Team</p>
                # """
                html=f"""
                    <html>
                    <head>
                        <style>
                            body {{
                                font-family: Arial, sans-serif;
                                background-color: #f4f4f4;
                                margin: 0;
                                padding: 0;
                                color: #333333;
                            }}
                            .container {{
                                max-width: 600px;
                                margin: 30px auto;
                                background-color: #ffffff;
                                padding: 20px;
                                border-radius: 8px;
                                box-shadow: 0 2px 6px rgba(0,0,0,0.1);
                            }}
                            h2 {{
                                color: #CE7E5A;
                            }}
                            .button {{
                                display: inline-block;
                                padding: 12px 24px;
                                margin: 20px 0;
                                background-color: #FE6244;
                                color: #ffffff;
                                text-decoration: none;
                                border-radius: 5px;
                                font-weight: bold;
                            }}
                            .footer {{
                                font-size: 12px;
                                color: #999999;
                                margin-top: 20px;
                            }}
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <h2>Password Reset Request</h2>
                            <p>Hello,</p>
                            <p>Your temporary password is: <strong>{temp_password}</strong></p>
                            <p>Click the button below to reset your password:</p>
                            <a href="{reset_link}" class="button">Reset Password</a>
                            <p>Best regards,<br>PROCG Team</p>
                            <div class="footer">
                                If you did not request this password reset, you can safely ignore this email.
                            </div>
                        </div>
                    </body>
                    </html>
                    """

            )
            mail.send(msg)
        except Exception as e:
            return jsonify({"error": f"Failed to send email: {str(e)}"}), 500

        return jsonify({"message": "Please check your email to reset your password."}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500



@users_bp.route("/reset_forgot_password", methods=["POST"])
def reset_forgot_password():
    data = request.json

    request_id = data.get("request_id")
    temp_pass = data.get("temporary_password")
    new_password = data.get("password")
    token = data.get("access_token")

    try:
        # ------------------ Decrypt & Decode JWT ------------------
        decrypted_token = decrypt(token, crypto_secret_key)

        decoded = decode_token(decrypted_token)
        user_id_from_token = decoded.get("sub")  # user_id is in 'sub'
        if not user_id_from_token:
            return jsonify({"message": "Invalid token."}), 403
        
        if decoded["exp"] < datetime.utcnow().timestamp():
            return jsonify({"message": "Reset link expired."}), 403

        # ------------------ Find Password Reset Request ------------------
        req_obj = ForgotPasswordRequest.query.filter_by(
            request_id=int(request_id),
            request_by=user_id_from_token,
            temporary_password=str(temp_pass),
            is_valid=True
        ).first()

        if not req_obj:
            return jsonify({"isSuccess": False, "message": "Invalid or expired temporary password."}), 400

        # ------------------ Update User Password ------------------
        user = DefUserCredential.query.get(user_id_from_token)
        if not user:
            return jsonify({"message": "User not found."}), 404

        user.password = generate_password_hash(
            new_password,
            method='pbkdf2:sha256',
            salt_length=16
        )
        user.last_update_date = datetime.utcnow()
        user.last_updated_by = user_id_from_token

        req_obj.is_valid = False
        req_obj.last_updated_date = datetime.utcnow()

        db.session.commit()


        return jsonify({"message": "Password updated successfully."})
    

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500



@users_bp.route("/verify_request", methods=["GET"])
def verify_request():
    try:
        # Get encrypted params from query string
        encrypted_request_id = request.args.get("request_id")
        encrypted_token = request.args.get("token")

        if not encrypted_request_id or not encrypted_token:
            return jsonify({"message": "Missing request_id or token"}), 400

        # ------------------ Decrypt ------------------
        try:
            request_id = int(decrypt(encrypted_request_id, crypto_secret_key))
            token = decrypt(encrypted_token, crypto_secret_key)
        except Exception:
            return jsonify({"message": "Invalid or corrupted link"}), 400

        # ------------------ Decode JWT ------------------
        try:
            decoded = decode_token(token)  # jwt validation (expiration included)
        except Exception:
            return jsonify({"message": "Invalid token"}), 403

        user_id_from_token = decoded.get("sub")
        if not user_id_from_token:
            return jsonify({"message": "Invalid token payload"}), 403

        # ------------------ Verify request in DB ------------------
        req_obj = ForgotPasswordRequest.query.filter(
            ForgotPasswordRequest.request_id == request_id,
            ForgotPasswordRequest.request_by == int(user_id_from_token),
            ForgotPasswordRequest.is_valid == True
        ).first()

        if not req_obj:
            return jsonify({"message": "The request is invalid"}), 200

        return jsonify({
            "message": "The request is valid",
            "result": {
                "request_id": req_obj.request_id,
                "email": req_obj.email,
                "temporary_password": req_obj.temporary_password
            }
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

