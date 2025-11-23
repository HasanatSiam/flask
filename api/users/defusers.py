from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefUser

from . import users_bp




@users_bp.route('/defusers', methods=['POST'])
@jwt_required()
def create_def_user():
    try:
        # Parse data from the request body
        data = request.get_json()
        # user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_address   = data['email_address']
        tenant_id       = data['tenant_id']
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }
        user_invitation_id = data.get('user_invitation_id')
        date_of_birth     = data.get('date_of_birth')

        # Duplicate check
        existing_user = DefUser.query.filter_by(email_address=email_address).first()
        if existing_user:
            return make_response(jsonify({"message": "Email address already exists"}), 409)
        

       # Convert the list of email addresses to a JSON-formatted string
       # email_addresses_json = json.dumps(email_addresses)  # Corrected variable name

       # Create a new ArcUser object
        new_user = DefUser(
        #   user_id         = user_id,
          user_name       = user_name,
          user_type       = user_type,
          email_address   = email_address,  # Corrected variable name
          created_by      = get_jwt_identity(),
          creation_date   = datetime.utcnow(),
          last_updated_by = get_jwt_identity(),
          last_update_date= datetime.utcnow(),
          tenant_id       = tenant_id,
          profile_picture = profile_picture,
          user_invitation_id = user_invitation_id,
          date_of_birth   = date_of_birth
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Added successfully",
                                       "User Id": new_user.user_id}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    

@users_bp.route('/defusers', methods=['GET'])
@jwt_required()
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    

@users_bp.route('/defusers/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_def_users(page, limit):
    try:
        query = DefUser.query.order_by(DefUser.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)



@users_bp.route('/defusers/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_users(page, limit):
    try:
        search_query = request.args.get('user_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefUser.query

        if search_query:
            query = query.filter(
                or_(
                    DefUser.user_name.ilike(f'%{search_query}%'),
                    DefUser.user_name.ilike(f'%{search_underscore}%'),
                    DefUser.user_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefUser.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching users", "error": str(e)}), 500)


# get a user by id
@users_bp.route('/defusers/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting user', 'error': str(e)}), 500)
    
    
@users_bp.route('/defusers/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'email_address' in data:
                user.email_address = data['email_address']
            if 'tenant_id' in data:
                user.tenant_id = data['tenant_id']
            if 'date_of_birth' in data:
                user.date_of_birth = data['date_of_birth']
            user.last_updated_by = get_jwt_identity()
            user.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)


@users_bp.route('/defusers/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)

