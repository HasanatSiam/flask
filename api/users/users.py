from datetime import datetime
from flask import request, jsonify, make_response
from werkzeug.security import generate_password_hash
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from utils.auth import role_required
from executors.models import(DefUser, 
                             DefPerson, DefUsersView, DefUserCredential, DefAccessProfile, NewUserInvitation)
from . import users_bp




@users_bp.route('/users', methods=['POST'])
@jwt_required()
def register_user():
    try:
        data = request.get_json()
        # Extract user fields
        # user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_address   = data['email_address']
        created_by      = get_jwt_identity()
        last_updated_by = get_jwt_identity()
        tenant_id       = data['tenant_id']
        date_of_birth   = data.get('date_of_birth')
        # Extract person fields
        first_name      = data.get('first_name')
        middle_name     = data.get('middle_name')
        last_name       = data.get('last_name')
        job_title_id       = data.get('job_title_id')
        user_invitation_id = data.get('user_invitation_id')
        # Extract credentials
        password        = data['password']

        # Set default profile picture if not provided
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }

        # Check for existing user/email
        # if DefUser.query.filter_by(user_name=user_name).first():
        #     return jsonify({"message": "Username already exists"}), 409
        # for email in email_address:
        #     if DefUser.query.filter(DefUser.email_address.contains    ([email])).first():
        #         return jsonify({"message": "Email already exists"}), 409

        # Check for existing username
        if DefUser.query.filter_by(user_name=user_name).first():
            return jsonify({"message": "Username already exists"}), 409

        # Check for existing email
        if DefUser.query.filter(DefUser.email_address == email_address).first():
            return jsonify({"message": "Email already exists"}), 409


        # Create user
        new_user = DefUser(
            # user_id         = user_id,
            user_name          = user_name,
            user_type          = user_type,
            email_address      = email_address,
            created_by         = created_by,
            creation_date      = datetime.utcnow(),
            last_updated_by    = last_updated_by,
            last_update_date   = datetime.utcnow(),
            tenant_id          = tenant_id,
            profile_picture    = profile_picture,
            user_invitation_id = user_invitation_id,
            date_of_birth      = date_of_birth
        )
        db.session.add(new_user)
        db.session.flush()

        # Create person if user_type is person
        if user_type.lower() == "person" :
            new_person = DefPerson(
                user_id          = new_user.user_id,
                first_name       = first_name,
                middle_name      = middle_name,
                last_name        = last_name,
                job_title_id     = job_title_id,
                created_by       = created_by,
                creation_date    = datetime.utcnow(),
                last_updated_by  = last_updated_by,
                last_update_date = datetime.utcnow()
            )
            db.session.add(new_person)


        
        # Create credentials
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        new_cred = DefUserCredential(
            user_id          = new_user.user_id,
            password         = hashed_password,
            created_by       = created_by,
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()


        )
        db.session.add(new_cred)

        if user_invitation_id:  
            user_invitation = NewUserInvitation.query.filter_by(user_invitation_id=user_invitation_id).first()
            if user_invitation:

                user_invitation.registered_user_id = new_user.user_id
                user_invitation.status             = "ACCEPTED"
                user_invitation.accepted_at        = datetime.utcnow()
        

        db.session.commit()
        return jsonify({"message": "Added successfully", "user_id": new_user.user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Registration failed", "error": str(e)}), 500



@users_bp.route('/users', methods=['GET'])
@jwt_required()
def get_users_unified():
    try:
        # Check for specific user ID
        user_id = request.args.get('user_id', type=int)
        if user_id:
            user = DefUsersView.query.filter_by(user_id=user_id).first()
            if user:
                return make_response(jsonify({"result" : user.json()}), 200)
            return make_response(jsonify({'message': 'User not found'}), 404)

        # Base query
        query = DefUsersView.query

        # Search filter
        user_name = request.args.get('user_name', '').strip()
        if user_name:
            query = query.filter(DefUsersView.user_name.ilike(f'%{user_name}%'))

        # Ordering
        query = query.order_by(DefUsersView.user_id.desc())

        # Pagination
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [user.json() for user in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)
        
        # Return all if no pagination
        users = query.all()
        return make_response(jsonify({"result": [user.json() for user in users]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching users', 'error': str(e)}), 500)


    

    

@users_bp.route('/users/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_specific_user(user_id):
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({'message': 'No input data provided'}), 400)

        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)
        

        # --- Username & Email uniqueness check ---
        new_user_name     = data.get('user_name')
        new_email_address = data.get('email_address')
        new_tenant_id     = data.get('tenant_id')
        new_date_of_birth = data.get('date_of_birth')

        if new_user_name or new_email_address:
            conflict_user = DefUser.query.filter(
                ((DefUser.user_name == new_user_name) | (DefUser.email_address == new_email_address)),
                DefUser.user_id != user_id
            ).first()
            if conflict_user:
                return make_response(jsonify({'message': 'Username or email already exists for another user'}), 400)

         # Update username/email after uniqueness check
        if new_user_name:
            user.user_name = new_user_name
        if new_email_address:
            user.email_address = new_email_address
        if new_tenant_id:
            user.tenant_id = new_tenant_id
        if new_date_of_birth:
            user.date_of_birth = new_date_of_birth

        # Update DefUser fields
        user.last_update_date = datetime.utcnow()
        user.last_updated_by  = get_jwt_identity()

        # Update DefPerson fields if user_type is "person"
        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if not person:
                return make_response(jsonify({'message': 'Person not found'}), 404)

            person.first_name       = data.get('first_name', person.first_name)
            person.middle_name      = data.get('middle_name', person.middle_name)
            person.last_name        = data.get('last_name', person.last_name)
            person.job_title_id     = data.get('job_title_id', person.job_title_id)
            person.last_update_date = datetime.utcnow()
            person.last_updated_by  = get_jwt_identity()
    

        # Password update logic
        password = data.get('password')
        if password:
            user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
            if not user_cred:
                return make_response(jsonify({'message': 'User credentials not found'}), 404)

            user_cred.password        = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
            user_cred.last_update_date= datetime.utcnow()
            user_cred.last_updated_by = get_jwt_identity()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)



@users_bp.route('/users/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_specific_user(user_id):
    try:
        # Find the user record in the DefUser table
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)

        access_profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
        for profile in access_profiles:
            db.session.delete(profile)


        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if person:
                db.session.delete(person)

        user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if user_credential:
            db.session.delete(user_credential)


        db.session.delete(user)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting user',
            'error': str(e)
        }), 500)


