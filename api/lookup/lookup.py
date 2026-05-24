from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_
from utils.auth import role_required
from executors.extensions import db
from executors.models import DefLookup, DefLookupValue, VwLookupWithValues
from . import lookup_bp


@lookup_bp.route('/def_lookup', methods=['POST'])
@jwt_required()
# @role_required()
def create_lookup():
    try:
        lookup_code = request.json.get('lookup_code')
        lookup_name = request.json.get('lookup_name')
        description = request.json.get('description')
        active_yn   = request.json.get('active_yn', 'Y')

        if not lookup_code or not lookup_name:
            return make_response(jsonify({"error": "lookup_code and lookup_name are required"}), 400)

        existing = DefLookup.query.filter_by(lookup_code=lookup_code).first()
        if existing:
            return make_response(jsonify({"error": f"Lookup code '{lookup_code}' already exists"}), 409)

        new_lookup = DefLookup(
            lookup_code      = lookup_code,
            lookup_name      = lookup_name,
            description      = description,
            active_yn        = active_yn,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow(),
        )
        db.session.add(new_lookup)
        db.session.commit()

        return make_response(jsonify({
            "message": "Added successfully",
            "lookup_id": new_lookup.lookup_id
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error creating lookup"}), 500)


@lookup_bp.route('/def_lookup', methods=['GET'])
@jwt_required()
# @role_required()
def get_lookups():
    try:
        lookup_id   = request.args.get('lookup_id', type=int)
        lookup_code = request.args.get('lookup_code', '').strip()
        page        = request.args.get('page', type=int)
        limit       = request.args.get('limit', type=int)

        if lookup_id is not None:
            record = DefLookup.query.filter_by(lookup_id=lookup_id).first()
            if not record:
                return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)
            return make_response(jsonify({"result": record.json()}), 200)

        query = DefLookup.query

        if lookup_code:
            lookup_code_space = lookup_code.replace('_', ' ')
            lookup_code_under = lookup_code.replace(' ', '_')
            query = query.filter(
                or_(
                    DefLookup.lookup_code.ilike(f'%{lookup_code}%'),
                    DefLookup.lookup_code.ilike(f'%{lookup_code_space}%'),
                    DefLookup.lookup_code.ilike(f'%{lookup_code_under}%'),
                    DefLookup.lookup_name.ilike(f'%{lookup_code}%'),
                )
            )

        query = query.order_by(DefLookup.lookup_id.desc())

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
        return make_response(jsonify({"error": str(e), "message": "Error fetching lookups"}), 500)


@lookup_bp.route('/def_lookup', methods=['PUT'])
@jwt_required()
# @role_required()
def update_lookup():
    try:
        lookup_id = request.args.get('lookup_id', type=int)
        if lookup_id is None:
            return make_response(jsonify({"error": "Query parameter 'lookup_id' is required"}), 400)

        lookup = DefLookup.query.filter_by(lookup_id=lookup_id).first()
        if not lookup:
            return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)

        if 'lookup_name' in request.json:
            lookup.lookup_name = request.json.get('lookup_name')
        if 'description' in request.json:
            lookup.description = request.json.get('description')
        if 'active_yn' in request.json:
            lookup.active_yn = request.json.get('active_yn')

        lookup.last_updated_by  = get_jwt_identity()
        lookup.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error updating lookup"}), 500)


@lookup_bp.route('/def_lookup', methods=['DELETE'])
@jwt_required()
# @role_required()
def delete_lookup():
    try:
        lookup_id = request.args.get('lookup_id', type=int)
        if lookup_id is None:
            return make_response(jsonify({"error": "Query parameter 'lookup_id' is required"}), 400)

        lookup = DefLookup.query.filter_by(lookup_id=lookup_id).first()
        if not lookup:
            return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)

        db.session.delete(lookup)
        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e), "message": "Error deleting lookup"}), 500)


# ── Combined create: lookup header + values in one request ────────────────────
#
# POST /def_lookup_with_values
# Body (JSON):
# {
#   "lookup_code"  : "PRIORITY",          -- required
#   "lookup_name"  : "Priority Levels",   -- required
#   "description"  : "...",               -- optional
#   "active_yn"    : "Y",                 -- optional, default "Y"
#   "values": [                           -- optional list of lookup values
#     { "value_code": "HIGH",  "value_label": "High",  "description": "", "sort_order": 1, "active_yn": "Y" },
#     { "value_code": "MEDIUM","value_label": "Medium","sort_order": 2 },
#     { "value_code": "LOW",   "value_label": "Low",   "sort_order": 3 }
#   ]
# }
@lookup_bp.route('/def_lookup_with_values', methods=['POST'])
@jwt_required()
# @role_required()
def create_lookup_with_values():
    try:
        data = request.get_json(force=True) or {}

        # ── Lookup header fields ──────────────────────────────────────────────
        lookup_code  = data.get('lookup_code', '').strip()
        lookup_name  = data.get('lookup_name', '').strip()
        description  = data.get('description')
        active_yn    = data.get('active_yn', 'Y')
        values_data  = data.get('values', [])        # list of value dicts

        if not lookup_code or not lookup_name:
            return make_response(
                jsonify({"error": "lookup_code and lookup_name are required"}), 400
            )

        if DefLookup.query.filter_by(lookup_code=lookup_code).first():
            return make_response(
                jsonify({"error": f"Lookup code '{lookup_code}' already exists"}), 409
            )

        now    = datetime.utcnow()
        user   = get_jwt_identity()

        # ── Create the lookup header ──────────────────────────────────────────
        new_lookup = DefLookup(
            lookup_code      = lookup_code,
            lookup_name      = lookup_name,
            description      = description,
            active_yn        = active_yn,
            created_by       = user,
            creation_date    = now,
            last_updated_by  = user,
            last_update_date = now,
        )
        db.session.add(new_lookup)
        db.session.flush()   # get new_lookup.lookup_id without committing yet

        # ── Validate & create each lookup value ───────────────────────────────
        seen_codes   = set()
        created_vals = []

        for idx, v in enumerate(values_data):
            value_code  = (v.get('value_code') or '').strip()
            value_label = (v.get('value_label') or '').strip()

            if not value_code or not value_label:
                db.session.rollback()
                return make_response(
                    jsonify({
                        "error"  : f"values[{idx}]: value_code and value_label are required",
                        "message": "Validation failed — nothing was saved"
                    }), 400
                )

            if value_code in seen_codes:
                db.session.rollback()
                return make_response(
                    jsonify({
                        "error"  : f"values[{idx}]: duplicate value_code '{value_code}' in request",
                        "message": "Validation failed — nothing was saved"
                    }), 400
                )
            seen_codes.add(value_code)

            lv = DefLookupValue(
                lookup_id        = new_lookup.lookup_id,
                value_code       = value_code,
                value_label      = value_label,
                description      = v.get('description'),
                sort_order       = idx + 1,   # auto-assigned by position in array
                active_yn        = v.get('active_yn', 'Y'),
                created_by       = user,
                creation_date    = now,
                last_updated_by  = user,
                last_update_date = now,
            )
            db.session.add(lv)
            created_vals.append(lv)

        db.session.commit()

        return make_response(
            jsonify({
                "message"          : "Added successfully",
                "lookup_id"        : new_lookup.lookup_id,
                "lookup_code"      : new_lookup.lookup_code,
                "values_created"   : len(created_vals),
                "lookup_value_ids" : [lv.lookup_value_id for lv in created_vals],
            }), 201
        )

    except Exception as e:
        db.session.rollback()
        return make_response(
            jsonify({"error": str(e), "message": "Error creating lookup with values"}), 500
        )


@lookup_bp.route('/lookup_with_values', methods=['GET'])
@jwt_required()
# @role_required()
def get_lookup_with_values():
    try:
        lookup_id   = request.args.get('lookup_id', type=int)
        lookup_code = request.args.get('lookup_code', '').strip()
        page        = request.args.get('page', type=int)
        limit       = request.args.get('limit', type=int)

        if lookup_id is not None:
            record = VwLookupWithValues.query.filter_by(lookup_id=lookup_id).first()
            if not record:
                return make_response(jsonify({"error": f"Lookup with id={lookup_id} not found"}), 404)
            return make_response(jsonify({"result": record.json()}), 200)

        query = VwLookupWithValues.query

        if lookup_code:
            lookup_code_space = lookup_code.replace('_', ' ')
            lookup_code_under = lookup_code.replace(' ', '_')
            query = query.filter(
                or_(
                    VwLookupWithValues.lookup_code.ilike(f'%{lookup_code}%'),
                    VwLookupWithValues.lookup_code.ilike(f'%{lookup_code_space}%'),
                    VwLookupWithValues.lookup_code.ilike(f'%{lookup_code_under}%'),
                    VwLookupWithValues.lookup_name.ilike(f'%{lookup_code}%'),
                )
            )

        query = query.order_by(VwLookupWithValues.lookup_id.desc())

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
        return make_response(jsonify({"error": str(e), "message": "Error fetching lookup with values"}), 500)
