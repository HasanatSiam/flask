from datetime import datetime
from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from utils.auth import role_required
from executors.extensions import db
from executors.models import DefMobileMenu
from . import mobile_menu_bp

@mobile_menu_bp.route('/def_mobile_menu', methods=['POST'])
@jwt_required()
@role_required()
def create_def_mobile_menu():
    try:
        data = request.get_json()
        new_menu = DefMobileMenu(
            menu_code=data.get('menu_code'),
            menu_name=data.get('menu_name'),
            menu_desc=data.get('menu_desc'),
            menu_structure=data.get('menu_structure'),
            created_by=get_jwt_identity(),
            last_updated_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        db.session.add(new_menu)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully', 'result': new_menu.json()}), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating mobile menu', 'error': str(e)}), 500)

@mobile_menu_bp.route('/def_mobile_menu', methods=['GET'])
@jwt_required()
@role_required()
def get_def_mobile_menu():
    try:
        menus = DefMobileMenu.query.order_by(DefMobileMenu.menu_id.desc()).all()
        return make_response(jsonify({'result': [m.json() for m in menus]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching mobile menus', 'error': str(e)}), 500)

@mobile_menu_bp.route('/def_mobile_menu', methods=['PUT'])
@jwt_required()
@role_required()
def update_def_mobile_menu():
    try:
        menu_id = request.args.get('menu_id', type=int)
        if not menu_id:
            return make_response(jsonify({'message': 'menu_id is required'}), 400)

        menu = DefMobileMenu.query.filter_by(menu_id=menu_id).first()
        if not menu:
            return make_response(jsonify({'message': 'Menu not found'}), 404)

        data = request.get_json()
        menu.menu_code = data.get('menu_code', menu.menu_code)
        menu.menu_name = data.get('menu_name', menu.menu_name)
        menu.menu_desc = data.get('menu_desc', menu.menu_desc)
        menu.menu_structure = data.get('menu_structure', menu.menu_structure)
        menu.last_updated_by = get_jwt_identity()
        menu.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully', 'result': menu.json()}), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating mobile menu', 'error': str(e)}), 500)

@mobile_menu_bp.route('/def_mobile_menu', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_def_mobile_menu():
    try:
        menu_id = request.args.get('menu_id', type=int)
        if not menu_id:
            return make_response(jsonify({'message': 'menu_id is required'}), 400)

        menu = DefMobileMenu.query.filter_by(menu_id=menu_id).first()
        if not menu:
            return make_response(jsonify({'message': 'Menu not found'}), 404)

        db.session.delete(menu)
        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error deleting mobile menu', 'error': str(e)}), 500)
