from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefLookup, DefLookupValue
from . import lookup_bp


@lookup_bp.route('/def_lookup_values', methods=['POST'])
@jwt_required()
# @role_required()
def create_lookup_value():
    try:
        lookup_id   = request.json.get('lookup_id')
        value_code  = request.json.get('value_code')
        value_label = request.json.get('value_label')
        description = request.json.get('description')
        sort_order  = request.json.get('sort_order', 1)
        active_yn   = request.json.get('active_yn', 'Y')

        if not lookup_id or not value_code or not value_label:
            return make_response(jsonify({"error": "lookup_id, value_code and value_label are required"}), 400)

        lookup = DefLookup.query.filter_by(lookup_id=lookup_id).first()
        if not lookup:
            return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)

        existing = DefLookupValue.query.filter_by(lookup_id=lookup_id, value_code=value_code).first()
        if existing:
            return make_response(jsonify({"error": f"value_code '{value_code}' already exists for this lookup"}), 409)

        new_value = DefLookupValue(
            lookup_id        = lookup_id,
            value_code       = value_code,
            value_label      = value_label,
            description      = description,
            sort_order       = sort_order,
            active_yn        = active_yn,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow(),
        )
        db.session.add(new_value)
        db.session.commit()

        return make_response(jsonify({
            "message": "Added successfully",
            "lookup_value_id": new_value.lookup_value_id
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error creating lookup value"}), 500)


@lookup_bp.route('/def_lookup_values', methods=['GET'])
@jwt_required()
# @role_required()
def get_lookup_values():
    try:
        lookup_value_id = request.args.get('lookup_value_id', type=int)
        lookup_id       = request.args.get('lookup_id', type=int)
        page            = request.args.get('page', type=int)
        limit           = request.args.get('limit', type=int)

        if lookup_value_id is not None:
            record = DefLookupValue.query.filter_by(lookup_value_id=lookup_value_id).first()
            if not record:
                return make_response(jsonify({"error": f"Lookup value with id={lookup_value_id} not found"}), 404)
            return make_response(jsonify({"result": record.json()}), 200)

        query = DefLookupValue.query

        if lookup_id is not None:
            lookup = DefLookup.query.filter_by(lookup_id=lookup_id).first()
            if not lookup:
                return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)
            query = query.filter_by(lookup_id=lookup_id)

        query = query.order_by(DefLookupValue.lookup_id, DefLookupValue.sort_order)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [r.json() for r in paginated.items],
                "total":  paginated.total,
                "pages":  paginated.pages,
                "page":   paginated.page,
            }), 200)

        records = query.all()
        return make_response(jsonify({"result": [r.json() for r in records]}), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e), "message": "Error fetching lookup values"}), 500)


@lookup_bp.route('/def_lookup_values', methods=['PUT'])
@jwt_required()
# @role_required()
def update_lookup_value():
    try:
        lookup_value_id = request.args.get('lookup_value_id', type=int)
        if lookup_value_id is None:
            return make_response(jsonify({"error": "Query parameter 'lookup_value_id' is required"}), 400)

        value = DefLookupValue.query.filter_by(lookup_value_id=lookup_value_id).first()
        if not value:
            return make_response(jsonify({"error": f"Lookup value with id={lookup_value_id} not found"}), 404)

        if 'value_label' in request.json:
            value.value_label = request.json.get('value_label')
        if 'description' in request.json:
            value.description = request.json.get('description')
        if 'sort_order' in request.json:
            value.sort_order = request.json.get('sort_order')
        if 'active_yn' in request.json:
            value.active_yn = request.json.get('active_yn')

        value.last_updated_by  = get_jwt_identity()
        value.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error updating lookup value"}), 500)


@lookup_bp.route('/def_lookup_values', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_lookup_value():
    try:
        lookup_value_id = request.args.get('lookup_value_id', type=int)
        if lookup_value_id is None:
            return make_response(jsonify({"error": "Query parameter 'lookup_value_id' is required"}), 400)

        value = DefLookupValue.query.filter_by(lookup_value_id=lookup_value_id).first()
        if not value:
            return make_response(jsonify({"error": f"Lookup value with id={lookup_value_id} not found"}), 404)

        db.session.delete(value)
        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error deleting lookup value"}), 500)
