from flask import jsonify, make_response, request
from flask_jwt_extended import jwt_required
from executors.models import DefUsersView
from utils.auth import role_required

from executors.extensions import db


from . import users_bp



@users_bp.route('/def_combined_user/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_combined_users(page, limit):
    try:
        query = DefUsersView.query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching users', 'error': str(e)}), 500)
        

@users_bp.route('/def_combined_user/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_combined_users(page, limit):
    user_name = request.args.get('user_name', '').strip()
    try:
        query = DefUsersView.query
        if user_name:
            query = query.filter(DefUsersView.user_name.ilike(f'%{user_name}%'))
        query = query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching users', 'error': str(e)}), 500)



