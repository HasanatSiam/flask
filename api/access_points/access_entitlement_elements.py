from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy import and_, or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessPoint,
    DefAccessEntitlement,
    DefAccessEntitlementElement
)
from . import access_points_bp


#Def_access_entitlement_elements

@access_points_bp.route('/def_access_entitlement_elements', methods=['POST'])
@jwt_required()
def create_entitlement_element():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'error': 'def_entitlement_id is required'}), 400)

        data = request.get_json()
        user_id = get_jwt_identity()

        # Validate entitlement
        entitlement = db.session.get(DefAccessEntitlement, def_entitlement_id)
        if not entitlement:
            return make_response(jsonify({'error': 'Invalid def_entitlement_id'}), 400)

        # Expect a list of access point IDs
        access_point_ids = data.get('def_access_point_ids') or [data.get('def_access_point_id')]
        if not access_point_ids or not isinstance(access_point_ids, list):
            return make_response(jsonify({'error': 'def_access_point_ids must be a list'}), 400)

        for access_point_id in access_point_ids:
            # Validate access point
            access_point = db.session.get(DefAccessPoint, access_point_id)
            if not access_point:
                return make_response(jsonify({'error': 'Invalid access point ID'}), 400)

            # Skip duplicates
            exists = (
                db.session.query(DefAccessEntitlementElement)
                .filter_by(def_entitlement_id=def_entitlement_id, def_access_point_id=access_point_id)
                .first()
            )
            if exists:
                continue

            # Add entitlement element
            db.session.add(DefAccessEntitlementElement(
                def_entitlement_id = def_entitlement_id,
                def_access_point_id = access_point_id,
                created_by = user_id,
                last_updated_by = user_id,
                creation_date = datetime.utcnow(),
                last_update_date = datetime.utcnow()
            ))


        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 400)





@access_points_bp.route('/def_access_entitlement_elements', methods=['GET'])
@jwt_required()
def get_entitlement_elements():
    try:
        # Query parameters
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        # def_access_point_id = request.args.get('def_access_point_id', type=int)

        query = DefAccessEntitlementElement.query

        if def_entitlement_id:
            query = query.filter(DefAccessEntitlementElement.def_entitlement_id == def_entitlement_id)
        # if def_access_point_id:
        #     query = query.filter(DefAccessEntitlementElement.def_access_point_id == def_access_point_id)

        # Return all filtered records (or all if no filter)
        elements = query.order_by(DefAccessEntitlementElement.creation_date.desc()).all()
        return make_response(jsonify([e.json() for e in elements]), 200)

    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 400)



@access_points_bp.route('/def_access_entitlement_elements', methods=['DELETE'])
@jwt_required()
def delete_entitlement_element():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        data = request.get_json()
        access_point_ids = data.get('def_access_point_ids')

        if not access_point_ids or not isinstance(access_point_ids, list):
            return make_response(jsonify({'message': 'def_access_point_ids (list) is required'}), 400)

        # Validate entitlement
        entitlement = db.session.get(DefAccessEntitlement, def_entitlement_id)
        if not entitlement:
            return make_response(jsonify({'error': 'Invalid def_entitlement_id'}), 400)

        db.session.query(DefAccessEntitlementElement).filter(
            DefAccessEntitlementElement.def_entitlement_id == def_entitlement_id,
            DefAccessEntitlementElement.def_access_point_id.in_(access_point_ids)
        ).delete(synchronize_session=False)

        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 400)



