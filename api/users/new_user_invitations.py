from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, create_access_token, decode_token
from werkzeug.security import generate_password_hash
from datetime import datetime, timedelta
from flask_mail import Message as MailMessage

from utils.auth import encrypt, decrypt
from config import crypto_secret_key, invitation_expire_time, mail, REACT_ENDPOINT_URL
from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import(DefUser,
                             DefPerson,
                             DefUserCredential,
                             NewUserInvitation,
                             DefPrivilege,
                             DefUserGrantedPrivilege,
                             DefRoles,
                             DefUserGrantedRole)

from . import users_bp

@users_bp.route("/invitations/via_email", methods=["POST"])
@jwt_required()
@role_required()
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
        expires = invitation_expire_time
        token = create_access_token(identity=str(invited_by), expires_delta=expires)
        encrypted_token = encrypt(token, crypto_secret_key)

        # Check for existing pending invite
        existing_invite = NewUserInvitation.query.filter_by(
            email=email, status="PENDING", type="EMAIL"
        ).first()

        if existing_invite and existing_invite.expires_at > datetime.utcnow():
            encrypted_id = encrypt(str(existing_invite.user_invitation_id), crypto_secret_key)
            # existing_invite.access_token is already encrypted — use it directly, do NOT re-encrypt
            existing_encrypted_token = existing_invite.access_token
            invite_link = f"{REACT_ENDPOINT_URL}/invitations/{encrypted_id}"
            return jsonify({
                "invitation_id": existing_invite.user_invitation_id,
                "token": existing_encrypted_token,
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
            access_token=encrypted_token,
            status="PENDING",
            type="EMAIL",
            created_by=int(invited_by),
            creation_date=datetime.utcnow(),
            last_updated_by=int(invited_by),
            last_update_date=datetime.utcnow(),
            expires_at=expires_at
        )
        db.session.add(new_invite)
        db.session.flush()

        # Encrypt invitation ID and token for the link
        encrypted_id = encrypt(str(new_invite.user_invitation_id), crypto_secret_key)
        invite_link = f"{REACT_ENDPOINT_URL}/invitations/{encrypted_id}"

        # Send email
        msg = MailMessage(
            subject="You're Invited to Join PROCG",
            recipients=[email],
            html=f"""
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 30px auto; padding: 20px; background: #fff; border-radius: 8px;">
                    <h2 style="color: #CE7E5A;">You're Invited to Join PROCG!</h2>
                    <p>Hello,</p>
                    <p>You've been invited to join the <strong>PROCG</strong> platform. Click the button below to accept your invitation and create your account.</p>
                    <a href="{invite_link}" style="display: inline-block; padding: 12px 24px; background: #FE6244; color: #fff; text-decoration: none; border-radius: 5px; font-weight: bold;">Accept Invitation</a>
                    <p style="margin-top: 20px;">Best regards,<br>PROCG Team</p>
                    <p style="font-size: 12px; color: #999;">This invitation expires in {int(invitation_expire_time.total_seconds() // 3600)} hour(s). If you did not expect this, you can safely ignore this email.</p>
                </div>
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
            "encrypted_id": encrypted_id,
            "invitation_link": invite_link,
            "message": "Invitation email sent successfully"
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e), "message": "Failed to send invitation"}), 500



@users_bp.route("/invitations/via_link", methods=["POST"])
@jwt_required()
@role_required()
def invitation_via_link():
    """Generate invitation link only"""
    try:
        current_user = get_jwt_identity()
        invited_by = current_user

        if not invited_by:
            return jsonify({"error": "Inviter ID required"}), 400

        expires = invitation_expire_time
        token = create_access_token(identity=str(invited_by), expires_delta=expires)
        expires_at = datetime.utcnow() + expires

        encrypted_token = encrypt(token, crypto_secret_key)
        new_invite = NewUserInvitation(
            invited_by=invited_by,
            access_token=encrypted_token,
            status="PENDING",
            type="LINK",
            created_by=int(invited_by),
            creation_date=datetime.utcnow(),
            last_updated_by=int(invited_by),
            last_update_date=datetime.utcnow(),
            expires_at=expires_at
        )
        db.session.add(new_invite)
        db.session.commit()

        encrypted_id = encrypt(str(new_invite.user_invitation_id), crypto_secret_key)
        invite_link = f"{REACT_ENDPOINT_URL}/invitations/{encrypted_id}"


        return jsonify({
            "success": True,
            "invitation_link": invite_link,
            "encrypted_id": encrypted_id,
            "message": "The invitation link was generated successfully"
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@users_bp.route("/invitations/<string:encrypted_id>", methods=["GET"])
def get_invitation_details(encrypted_id):
    try:
        try:
            invitation_id = int(decrypt(encrypted_id, crypto_secret_key))
        except Exception:
            return jsonify({"valid": False, "message": "Invalid invitation link"}), 400

        invite = NewUserInvitation.query.filter_by(
            user_invitation_id=invitation_id
        ).first()

        if not invite:
            return jsonify({"valid": False, "message": "No invitation found"}), 404

        if invite.status != "PENDING" or invite.expires_at < datetime.utcnow():
            invite.status = "EXPIRED"
            db.session.commit()
            return jsonify({"valid": False, "message": "Invitation expired"}), 200

        # Validate the stored token
        try:
            decrypted_token = decrypt(invite.access_token, crypto_secret_key)
            decoded = decode_token(decrypted_token)
        except Exception as e:
            msg = str(e).lower()
            if "expired" in msg:
                return jsonify({"valid": False, "message": "Token expired"}), 401
            return jsonify({"valid": False, "message": "Invalid token"}), 403

        return jsonify({
            "valid": True,
            "invited_by": invite.invited_by,
            "email": invite.email,
            "type": invite.type,
            "message": "Invitation link is valid"
        }), 200

    except Exception as e:
        return jsonify({"valid": False, "message": str(e)}), 500

@users_bp.route("/invitations/accept/<encrypted_id>", methods=["POST"])
def accept_invitation(encrypted_id):
    try:
        # Decrypt invitation ID
        try:
            user_invitation_id = int(decrypt(encrypted_id, crypto_secret_key))
        except Exception:
            return jsonify({"message": "Invalid or corrupted invitation link"}), 400

        data = request.get_json() or {}
        required_fields = ["user_name", "user_type", "email_address", "tenant_id", "password"]
        missing = [f for f in required_fields if not data.get(f)]
        if missing:
            return jsonify({"message": f"Missing required fields: {', '.join(missing)}"}), 400

        # Fetch invite from DB and get token from there
        invite = NewUserInvitation.query.filter_by(
            user_invitation_id=user_invitation_id
        ).first()

        if not invite or invite.status in ["ACCEPTED", "EXPIRED"] or (invite.expires_at and invite.expires_at < datetime.utcnow()):
            if invite:
                invite.status = "EXPIRED"
                db.session.commit()
            return jsonify({"message": "This invitation is not valid"}), 400

        # Decode JWT token from DB
        try:
            decrypted_token = decrypt(invite.access_token, crypto_secret_key)
            decoded = decode_token(decrypted_token)
        except Exception as e:
            msg = str(e).lower()
            if "expired" in msg:
                return jsonify({"message": "Token has expired"}), 401
            return jsonify({"message": "Invalid token"}), 403

        inviter_id = decoded.get("sub")
        if not inviter_id:
            return jsonify({"message": "Missing inviter info in token"}), 403

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
                created_by=new_user.user_id,
                last_updated_by=new_user.user_id,
                creation_date=datetime.utcnow(),
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_person)

        # Create credentials
        hashed_password = generate_password_hash(data["password"], method="pbkdf2:sha256", salt_length=16)
        new_cred = DefUserCredential(
            user_id=new_user.user_id,
            password=hashed_password,
            created_by=new_user.user_id,
            last_updated_by=new_user.user_id,
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        db.session.add(new_cred)

        # Update invitation
        invite.registered_user_id = new_user.user_id
        invite.status             = "ACCEPTED"
        invite.accepted_at        = datetime.utcnow()
        invite.last_updated_by    = new_user.user_id
        invite.last_update_date   = datetime.utcnow()

        if data["user_type"].lower() == "person":
            # Add default 'query' privilege
            query_priv = DefPrivilege.query.filter(DefPrivilege.privilege_name.ilike('query')).first()
            if query_priv:
                new_priv_mapping = DefUserGrantedPrivilege(
                    user_id=new_user.user_id,
                    privilege_id=query_priv.privilege_id,
                    created_by=inviter_id,
                    creation_date=datetime.utcnow(),
                    last_updated_by=inviter_id,
                    last_update_date=datetime.utcnow()
                )
                db.session.add(new_priv_mapping)

            # Add default 'User' role
            user_role = DefRoles.query.filter(DefRoles.role_name.ilike('user')).first()
            if user_role:
                new_role_mapping = DefUserGrantedRole(
                    user_id=new_user.user_id,
                    role_id=user_role.role_id,
                    created_by=inviter_id,
                    creation_date=datetime.utcnow(),
                    last_updated_by=inviter_id,
                    last_update_date=datetime.utcnow()
                )
                db.session.add(new_role_mapping)

        db.session.commit()

        return jsonify({"message": "Invitation accepted, user created successfully", "user_id": new_user.user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error processing invitation", "error": str(e)}), 500

