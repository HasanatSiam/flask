from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessEntitlement,
    DefAccessEntitlementElement,

)
from . import access_points_bp


#def_access_entitlements
@access_points_bp.route('/def_access_entitlements', methods=['GET'])
@jwt_required()
def get_access_entitlements():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        entitlement_name = request.args.get('entitlement_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_entitlement_id:
            entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
            if entitlement:
                return make_response(jsonify({'result': entitlement.json()}), 200)
            return make_response(jsonify({'message': 'Entitlement not found'}), 404)

        query = DefAccessEntitlement.query

        # Case 2: Search
        if entitlement_name:
            search_query = entitlement_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_query}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_underscore}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [e.json() for e in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        entitlements = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).all()
        return make_response(jsonify({'result': [e.json() for e in entitlements]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@access_points_bp.route('/def_access_entitlements/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_entitlements(page, limit):
    try:
        paginated = DefAccessEntitlement.query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [e.json() for e in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@access_points_bp.route('/def_access_entitlements', methods=['POST'])
@jwt_required()
def create_entitlement():
    try:
        new_e = DefAccessEntitlement(
            entitlement_name = request.json.get('entitlement_name'),
            description = request.json.get('description'),
            comments = request.json.get('comments'),
            status = request.json.get('status'),
            effective_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_e)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating entitlement', 'error': str(e)}), 500)


@access_points_bp.route('/def_access_entitlements', methods=['PUT'])
@jwt_required()
def update_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
        if entitlement:
            entitlement.entitlement_name = request.json.get('entitlement_name', entitlement.entitlement_name)
            entitlement.description = request.json.get('description', entitlement.description)
            entitlement.comments = request.json.get('comments', entitlement.comments)
            entitlement.status = request.json.get('status', entitlement.status)
            entitlement.effective_date = datetime.utcnow()
            entitlement.revision =  entitlement.revision + 1
            entitlement.revision_date = datetime.utcnow()
            entitlement.last_updated_by = get_jwt_identity()
            entitlement.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing entitlement', 'error': str(e)}), 500)


@access_points_bp.route('/def_access_entitlements', methods=['DELETE'])
@jwt_required()
def delete_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
        if entitlement:
            db.session.delete(entitlement)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting entitlement', 'error': str(e)}), 500)


@access_points_bp.route('/def_access_entitlements/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return jsonify({'message': 'def_entitlement_id is required'}), 400

        entitlement_exists = db.session.query(db.exists().where(DefAccessEntitlement.def_entitlement_id == def_entitlement_id)).scalar()

        entitlement_elements_exists = db.session.query(db.exists().where(DefAccessEntitlementElement.def_access_entitlement_id == def_entitlement_id)).scalar()

        if not entitlement_exists and not entitlement_elements_exists:
            return jsonify({'error': f'No records found in def_access_entitlements or def_access_entitlement_elements for ID {def_entitlement_id}'}), 404

        DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).delete(synchronize_session=False)

        DefAccessEntitlementElement.query.filter_by(def_entitlement_id=def_entitlement_id).delete(synchronize_session=False)


        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


