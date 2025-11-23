from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import or_, func
from datetime import datetime
from utils.auth import role_required
from executors.extensions import db
from executors.models import DefPerson
from . import users_bp




@users_bp.route('/defpersons', methods=['POST'])
@jwt_required()
def create_arc_person():
    try:
        data = request.get_json()
        user_id     = data['user_id']
        first_name  = data['first_name']
        middle_name = data['middle_name']
        last_name   = data['last_name']
        job_title_id = data['job_title_id']  
        
        # create arc persons object 
        person =  DefPerson(
            user_id          = user_id,
            first_name       = first_name,
            middle_name      = middle_name,
            last_name        = last_name,
            job_title_id     = job_title_id,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        ) 
        
        # Add arc persons data to the database session
        db.session.add(person)
        # Commit the changes to the database
        db.session.commit()
        # Return a success response
        return make_response(jsonify({"message": "Added successfully"}), 201)
    
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
    
@users_bp.route('/defpersons', methods=['GET'])
@jwt_required()
def get_persons():
    try:
        persons = DefPerson.query.all()
        return make_response(jsonify([person.json() for person in persons]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting persons', 'error': str(e)}), 500)
    



@users_bp.route('/defpersons/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_persons(page, limit):
    try:
        query = DefPerson.query.order_by(DefPerson.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting persons', 'error': str(e)}), 500)

@users_bp.route('/defpersons/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_persons(page, limit):
    try:
        search_query = request.args.get('name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefPerson.query

        if search_query:
            query = query.filter(
                or_(
                    func.lower(DefPerson.first_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_space}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefPerson.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching persons", "error": str(e)}), 500)


@users_bp.route('/defpersons/<int:user_id>', methods=['GET'])
@jwt_required()
def get_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            return make_response(jsonify({'person': person.json()}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting person', 'error': str(e)}), 500) 


@users_bp.route('/defpersons/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            data = request.get_json()
            # Update only the fields provided in the JSON data
            if 'first_name' in data:
                person.first_name = data['first_name']
            if 'middle_name' in data:
                person.middle_name = data['middle_name']
            if 'last_name' in data:
                person.last_name = data['last_name']
            if 'job_title_id' in data:
                person.job_title_id = data['job_title_id']
            person.last_updated_by = get_jwt_identity()
            person.last_udpate_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing person', 'error': str(e)}), 500)
    
    
@users_bp.route('/defpersons/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            db.session.delete(person)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)

