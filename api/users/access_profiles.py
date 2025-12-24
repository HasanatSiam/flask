from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from executors.extensions import db
from sqlalchemy import func
from utils.auth import role_required
from executors.models import (DefUser,
                              DefAccessProfile)
from . import users_bp

@users_bp.route('/access_profiles/<int:user_id>', methods=['POST'])
@jwt_required()
def create_access_profiles(user_id):
    try:
        profile_type = request.json.get('profile_type') # Fixed incorrect key
        profile_id = request.json.get('profile_id')
        primary_yn = request.json.get('primary_yn', 'N') # Default to 'N' if not provided


        if not profile_type or not profile_id:
            return make_response(jsonify({"message": "Missing required fields"}), 400)

        # Check if user_id exists in def_users
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"message": f"User with ID {user_id} not found in def_users"}), 404)

        # Check if profile_id exists in DefAccessProfile or DefUser.email_address (case-insensitive)
        existing_profile = DefAccessProfile.query.filter(func.lower(DefAccessProfile.profile_id) == profile_id.lower()).first()
        if existing_profile:
            return make_response(jsonify({"message": f"Email '{profile_id}' already exists in DefAccessProfile"}), 409)

        existing_user = DefUser.query.filter(func.lower(DefUser.email_address) == profile_id.lower()).first()
        if existing_user:
            return make_response(jsonify({"message": f"Email '{profile_id}' already exists in DefUser"}), 409)

        new_profile = DefAccessProfile(
            user_id          = user_id,
            profile_type     = profile_type,
            profile_id       = profile_id,
            primary_yn       = primary_yn,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()

        )

        db.session.add(new_profile)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

    except IntegrityError as e:
        db.session.rollback()
        print("IntegrityError:", str(e))
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 409)

    except Exception as e:
        db.session.rollback()
        print("General Exception:", str(e))
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 500)


# Get all access profiles
@users_bp.route('/access_profiles', methods=['GET'])
@jwt_required()
def get_users_access_profiles():
    try:
        profiles = DefAccessProfile.query.all()
        return make_response(jsonify([profile.json() for profile in profiles]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Access Profiles", "error": str(e)}), 500)


@users_bp.route('/access_profiles/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user_access_profiles_(user_id):
    try:
        profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
        
        if profiles:
            return make_response(jsonify([profile.json() for profile in profiles]), 200)
        else:
            return make_response(jsonify({"message": "Access Profiles not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Access Profiles", "error": str(e)}), 500)


@users_bp.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['PUT'])
@jwt_required()
def update_access_profile(user_id, serial_number):
    try:
        # Retrieve the existing access profile
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if not profile:
            return make_response(jsonify({"message": "Access Profile not found"}), 404)

        data = request.get_json()

        # Update fields in DefAccessProfile table
        if 'profile_type' in data:
            profile.profile_type = data['profile_type']
        if 'profile_id' in data:
            profile.profile_id = data['profile_id']
        if 'primary_yn' in data:
            profile.primary_yn = data['primary_yn']
        profile.last_updated_by = get_jwt_identity()
        profile.last_update_date = datetime.utcnow()

        # Commit changes to DefAccessProfile
        db.session.commit()

        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on error
        return make_response(jsonify({"message": "Error Editing Access Profile", "error": str(e)}), 500)


# Delete an access profile
@users_bp.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['DELETE'])
@jwt_required()
def delete_access_profile(user_id, serial_number):
    try:
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if profile:
            db.session.delete(profile)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
        return make_response(jsonify({"message": "Access Profile not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting Access Profile", "error": str(e)}), 500)

