from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, create_access_token, decode_token
from werkzeug.security import generate_password_hash
from datetime import datetime, timedelta
from flask_mail import Message as MailMessage

from utils.auth import encrypt, decrypt
from config import crypto_secret_key, invitation_expire_time, mail
from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import(DefUser,
                             DefPerson,
                             DefUserCredential,
                             NewUserInvitation)

from . import users_bp

@users_bp.route("/invitations/via_email", methods=["POST"])
@jwt_required()
def invitation_via_email():
    """Send invitation via email with encrypted links"""
    try:
        current_user = get_jwt_identity()
        data = request.get_json() or {}
        invited_by = data.get("invited_by") or current_user
        email = data.get("email")

        if not invited_by or not email:
            return jsonify({"error": "Inviter ID and email required"}), 400

        # Check if user already exists
        if DefUser.query.filter_by(email_address=email).first():
            return jsonify({"message": "User with this email already exists"}), 200

        # Token expiration and generation
        expires = timedelta(minutes=invitation_expire_time)
        token = create_access_token(identity=str(invited_by), expires_delta=expires)
        encrypted_token = encrypt(token, crypto_secret_key)

        # Check for existing pending invite
        existing_invite = NewUserInvitation.query.filter_by(
            email=email, status="PENDING", type="EMAIL"
        ).first()

        if existing_invite and existing_invite.expires_at > datetime.utcnow():
            encrypted_id = encrypt(str(existing_invite.user_invitation_id), crypto_secret_key)
            encrypted_existing_token = encrypt(existing_invite.token, crypto_secret_key)
            invite_link = f"{request.host_url}invitation/{encrypted_id}/{encrypted_existing_token}"
            return jsonify({
                "invitation_id": existing_invite.user_invitation_id,
                "token": encrypted_existing_token,
                "invitation_link": invite_link,
                "message": "Pending invitation already exists"
            }), 200
        elif existing_invite:
            existing_invite.status = "EXPIRED"
            db.session.commit()

        # Create new invitation
        expires_at = datetime.utcnow() + expires
        new_invite = NewUserInvitation(
            invited_by=invited_by,
            email=email,
            token=encrypted_token,  # store encrypted token in DB
            status="PENDING",
            type="EMAIL",
            created_at=datetime.utcnow(),
            expires_at=expires_at
        )
        db.session.add(new_invite)
        db.session.flush()

        # Encrypt invitation ID and token for the link
        encrypted_id = encrypt(str(new_invite.user_invitation_id), crypto_secret_key)
        invite_link = f"{request.host_url}invitations/{encrypted_id}/{encrypted_token}"

        # Send email
        msg = MailMessage(
            subject="You're Invited to Join PROCG",
            recipients=[email],
            html=f"""
            <p>Hello,</p>
            <p>You’ve been invited to join PROCG!</p>
            <p>Click below to accept your invitation:</p>
            <p><a href="{invite_link}">Accept Invitation</a></p>
            <p>This link expires in {invitation_expire_time} minute(s).</p>
            <p>— The PROCG Team</p>
            """
        )

        try:
            mail.send(msg)
        except Exception as mail_error:
            db.session.rollback()
            return jsonify({"error": str(mail_error), "message": "Failed to send email."}), 500

        db.session.commit()

        return jsonify({
            "success": True,
            "invitation_id": new_invite.user_invitation_id,
            "token": encrypted_token,
            "encrypted_id": encrypted_id,
            "invitation_link": invite_link,
            "message": "Invitation email sent successfully"
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e), "message": "Failed to send invitation"}), 500



@users_bp.route("/invitations/via_link", methods=["POST"])
@jwt_required()
def invitation_via_link():
    """Generate invitation link only"""
    try:
        current_user = get_jwt_identity()
        invited_by = current_user

        if not invited_by:
            return jsonify({"error": "Inviter ID required"}), 400

        expires = timedelta(hours=invitation_expire_time)
        token = create_access_token(identity=str(invited_by), expires_delta=expires)
        expires_at = datetime.utcnow() + expires

        encrypted_token = encrypt(token, crypto_secret_key)
        new_invite = NewUserInvitation(
            invited_by=invited_by,
            token=encrypted_token,
            status="PENDING",
            type="LINK",
            created_at=datetime.utcnow(),
            expires_at=expires_at
        )
        db.session.add(new_invite)
        db.session.commit()

        encrypted_id = encrypt(str(new_invite.user_invitation_id), crypto_secret_key)
        invite_link = f"{request.host_url}invitations/{encrypted_id}/{encrypted_token}"


        return jsonify({
            "success": True,
            "invitation_link": invite_link,
            "encrypted_id": encrypted_id,
            "token": encrypted_token,
            "message": "The invitation link was generated successfully"
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@users_bp.route("/invitations/<string:encrypted_id>/<string:token>", methods=["GET"])
def get_invitation_details(encrypted_id, token):
    try:
        try:
            invitation_id = int(decrypt(encrypted_id, crypto_secret_key))
            decrypted_token = decrypt(token, crypto_secret_key)
        except Exception:
            return jsonify({"valid": False, "message": "Invalid invitation link"}), 400

        try:
            decoded = decode_token(decrypted_token)
            invited_by = decoded.get("sub")
        except Exception as e:
            msg = str(e).lower()
            if "expired" in msg:
                return jsonify({"valid": False, "message": "Token expired"}), 401
            return jsonify({"valid": False, "message": "Invalid token"}), 403

        invite = NewUserInvitation.query.filter_by(
            user_invitation_id=invitation_id,
            invited_by=invited_by,
            token=token
        ).first()

        if not invite:
            return jsonify({"valid": False, "message": "No invitation found"}), 404

        if invite.status != "PENDING" or invite.expires_at < datetime.utcnow():
            invite.status = "EXPIRED"
            db.session.commit()
            return jsonify({"valid": False, "message": "Invitation expired"}), 200

        return jsonify({
            "valid": True,
            "invited_by": invite.invited_by,
            "email": invite.email,
            "type": invite.type,
            "message": "Invitation link is valid"
        }), 200

    except Exception as e:
        return jsonify({"valid": False, "message": str(e)}), 500

@users_bp.route("/invitations/accept/<encrypted_id>/<token>", methods=["POST"])
def accept_invitation(encrypted_id, token):
    try:
        # Decrypt invitation ID and Token
        try:
            user_invitation_id = int(decrypt(encrypted_id, crypto_secret_key))
            decrypted_token = decrypt(token, crypto_secret_key)
        except Exception:
            return jsonify({"message": "Invalid or corrupted invitation link"}), 400

        data = request.get_json() or {}
        required_fields = ["user_name", "user_type", "email_address", "tenant_id", "password"]
        missing = [f for f in required_fields if not data.get(f)]
        if missing:
            return jsonify({"message": f"Missing required fields: {', '.join(missing)}"}), 400

        # Decode JWT token
        try:
            decoded = decode_token(decrypted_token)
        except Exception as e:
            msg = str(e).lower()
            if "expired" in msg:
                return jsonify({"message": "Token has expired"}), 401
            return jsonify({"message": "Invalid token"}), 403

        inviter_id = decoded.get("sub")
        if not inviter_id:
            return jsonify({"message": "Missing inviter info in token"}), 403

        # Check invitation
        invite = NewUserInvitation.query.filter_by(
            user_invitation_id=user_invitation_id,
            invited_by=inviter_id,
            token=token
        ).first()

        if not invite or invite.status in ["ACCEPTED", "EXPIRED"] or (invite.expires_at and invite.expires_at < datetime.utcnow()):
            if invite:
                invite.status = "EXPIRED"
                db.session.commit()
            return jsonify({"message": "This invitation is not valid"}), 400

        # Check existing username/email
        if DefUser.query.filter_by(user_name=data["user_name"]).first():
            return jsonify({"message": "Username already exists"}), 409
        if DefUser.query.filter_by(email_address=data["email_address"]).first():
            return jsonify({"message": "Email already exists"}), 409

        # Create user
        new_user = DefUser(
            user_name=data["user_name"],
            user_type=data["user_type"],
            email_address=data["email_address"],
            tenant_id=data["tenant_id"],
            date_of_birth=data.get("date_of_birth"),
            created_by=inviter_id,
            last_updated_by=inviter_id,
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow(),
            user_invitation_id=user_invitation_id,
            profile_picture=data.get("profile_picture") or {
                "original": "uploads/profiles/default/profile.jpg",
                "thumbnail": "uploads/profiles/default/thumbnail.jpg"
            }
        )
        db.session.add(new_user)
        db.session.flush()  # get new_user.user_id

        # Create person if user_type is person
        if data["user_type"].lower() == "person":
            new_person = DefPerson(
                user_id=new_user.user_id,
                first_name=data.get("first_name"),
                middle_name=data.get("middle_name"),
                last_name=data.get("last_name"),
                job_title_id=data.get("job_title_id"),
                created_by=inviter_id,
                last_updated_by=inviter_id,
                creation_date=datetime.utcnow(),
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_person)

        # Create credentials
        hashed_password = generate_password_hash(data["password"], method="pbkdf2:sha256", salt_length=16)
        new_cred = DefUserCredential(
            user_id=new_user.user_id,
            password=hashed_password,
            created_by=inviter_id,
            last_updated_by=inviter_id,
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        db.session.add(new_cred)

        # Update invitation
        invite.registered_user_id = new_user.user_id
        invite.status = "ACCEPTED"
        invite.accepted_at = datetime.utcnow()

        db.session.commit()

        return jsonify({"message": "Invitation accepted, user created successfully", "user_id": new_user.user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error processing invitation", "error": str(e)}), 500

