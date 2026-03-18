from flask import request, jsonify
from flask_jwt_extended import create_access_token, decode_token
from flask_mail import Message as MailMessage
from datetime import datetime
import random

from config import crypto_secret_key, invitation_expire_time, mail
from utils.auth import encrypt, decrypt
from executors.models import DefUsersView, ForgotPasswordRequest, DefUserCredential
from executors.extensions import db
from werkzeug.security import generate_password_hash
from . import users_bp


@users_bp.route("/forgot-password/request", methods=["POST"])
def create_request():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Request body must be JSON."}), 400

        user_name = data.get("user_name")
        email     = data.get("email_address")
        dob       = data.get("date_of_birth")

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
        additional_claims = {"user_id": user.user_id}
        token = create_access_token(
            identity=str(user.user_id),
            additional_claims=additional_claims,
            expires_delta=invitation_expire_time
        )
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

        encrypted_req_id  = encrypt(str(req_obj.forgot_password_request_id), crypto_secret_key)
        encrypted_user_id = encrypt(str(user.user_id), crypto_secret_key)

        reset_link = f"{request.host_url}reset-password/{encrypted_req_id}/{encrypted_user_id}/{encrypted_token}"

        # ------------------ Send Email ------------------
        try:
            msg = MailMessage(
                subject="You're invited to reset your password",
                recipients=[email],
                html=f"""
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 30px auto; padding: 20px; background: #fff; border-radius: 8px;">
                        <h2 style="color: #CE7E5A;">Password Reset Request</h2>
                        <p>Hello,</p>
                        <p>Your temporary password is: <strong>{temp_password}</strong></p>
                        <p>Click the button below to reset your password:</p>
                        <a href="{reset_link}" style="display: inline-block; padding: 12px 24px; background: #FE6244; color: #fff; text-decoration: none; border-radius: 5px; font-weight: bold;">Reset Password</a>
                        <p style="margin-top: 20px;">Best regards,<br>PROCG Team</p>
                        <p style="font-size: 12px; color: #999;">If you did not request this, you can safely ignore this email.</p>
                    </div>
                """
            )
            mail.send(msg)
        except Exception as e:
            return jsonify({"error": f"Failed to send email: {str(e)}"}), 500

        return jsonify({"message": "Please check your email to reset your password."}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@users_bp.route("/forgot-password/verify", methods=["GET"])
def verify_request():
    try:
        encrypted_request_id = request.args.get("forgot_password_request_id")
        encrypted_token      = request.args.get("access_token")

        if not encrypted_request_id or not encrypted_token:
            return jsonify({"message": "Missing forgot_password_request_id or access_token"}), 400

        # ------------------ Decrypt ------------------
        try:
            request_id = int(decrypt(encrypted_request_id, crypto_secret_key))
            token      = decrypt(encrypted_token, crypto_secret_key)
        except Exception:
            return jsonify({"is_valid": False, "message": "Invalid or corrupted link"}), 400

        # ------------------ Decode JWT ------------------
        try:
            decoded = decode_token(token)
        except Exception:
            return jsonify({"is_valid": False, "message": "Invalid token"}), 403

        user_id_from_token = decoded.get("user_id")
        if not user_id_from_token:
            return jsonify({"is_valid": False, "message": "Invalid token payload"}), 403

        # ------------------ Verify request in DB ------------------
        req_obj = ForgotPasswordRequest.query.filter(
            ForgotPasswordRequest.forgot_password_request_id == request_id,
            ForgotPasswordRequest.request_by == user_id_from_token,
            ForgotPasswordRequest.is_valid == True
        ).first()

        if not req_obj:
            return jsonify({"is_valid": False, "message": "The request is invalid"}), 200

        return jsonify({
            "is_valid": True,
            "message": "The request is valid",
            "result": {
                "forgot_password_request_id": req_obj.forgot_password_request_id,
                "email":              req_obj.email,
                "temporary_password": req_obj.temporary_password
            }
        }), 200

    except Exception as e:
        return jsonify({"is_valid": False, "error": str(e)}), 500


@users_bp.route("/forgot-password/reset", methods=["POST"])
def reset_forgot_password():
    data = request.json

    request_id   = data.get("forgot_password_request_id")
    temp_pass    = data.get("temporary_password")
    new_password = data.get("password")
    token        = data.get("access_token")

    try:
        # ------------------ Decrypt & Decode JWT ------------------
        decrypted_token    = decrypt(token, crypto_secret_key)
        decoded            = decode_token(decrypted_token)
        user_id_from_token = decoded.get("user_id")

        if not user_id_from_token:
            return jsonify({"is_success": False, "message": "Invalid token."}), 403

        if decoded["exp"] < datetime.utcnow().timestamp():
            return jsonify({"is_success": False, "message": "Reset link expired."}), 403

        # ------------------ Find Password Reset Request ------------------
        req_obj = ForgotPasswordRequest.query.filter_by(
            forgot_password_request_id=int(request_id),
            request_by=user_id_from_token,
            temporary_password=str(temp_pass),
            is_valid=True
        ).first()

        if not req_obj:
            return jsonify({"is_success": False, "message": "Invalid or expired temporary password."}), 400

        # ------------------ Update User Password ------------------
        user = DefUserCredential.query.get(user_id_from_token)
        if not user:
            return jsonify({"is_success": False, "message": "User not found."}), 404

        user.password         = generate_password_hash(new_password, method='pbkdf2:sha256', salt_length=16)
        user.last_update_date = datetime.utcnow()
        user.last_updated_by  = user_id_from_token

        req_obj.is_valid          = False
        req_obj.last_update_date   = datetime.utcnow()

        db.session.commit()

        return jsonify({"is_success": True, "message": "Password updated successfully."})

    except Exception as e:
        db.session.rollback()
        return jsonify({"is_success": False, "error": str(e)}), 500
