from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime

from sqlalchemy import or_, func
from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefAccessPoint,
    DefDataSource,
    DefAccessEntitlementElement,
    DefAccessPointsV
)
from . import access_points_bp


#def_access_points

@access_points_bp.route("/def_access_points", methods=["POST"])
@jwt_required()
def create_access_point():
    try:
        data = request.get_json() or {}

        access_point_name = data.get("access_point_name")
        description = data.get("description")
        platform = data.get("platform")
        access_point_type = data.get("access_point_type")
        access_control = data.get("access_control")
        change_control = data.get("change_control")
        audit = data.get("audit")
        def_data_source_id = data.get("def_data_source_id")
        def_entitlement_id = data.get("def_entitlement_id")


        data_source = db.session.query(DefDataSource).filter_by(def_data_source_id=def_data_source_id).first()
        if not data_source:
            return make_response(jsonify({
                "message": "Invalid data source ID",
                "error": "Data source not found"
            }), 400)

        access_point = DefAccessPoint(
            def_data_source_id = def_data_source_id,
            access_point_name = access_point_name,
            description = description,
            platform = platform,
            access_point_type = access_point_type,
            access_control = access_control,
            change_control = change_control,
            audit = audit,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(access_point)
        db.session.flush()

        entitlement_element = DefAccessEntitlementElement(
            def_entitlement_id = def_entitlement_id,
            def_access_point_id = access_point.def_access_point_id,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        
        db.session.add(entitlement_element)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating access point', 'error': str(e)}), 500)



@access_points_bp.route("/def_access_points", methods=["GET"])
@jwt_required()
def get_access_points():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        access_point_name = request.args.get("access_point_name", type=str)
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)

        query = DefAccessPoint.query

        # Filter by access_point_name if provided
        if access_point_name:
            query = query.filter(DefAccessPoint.access_point_name.ilike(f"%{access_point_name}%"))

        # Fetch single access point
        if def_access_point_id:
            access_point = query.filter_by(def_access_point_id=def_access_point_id).first()
            if not access_point:
                return make_response(jsonify({
                    "message": "Access point not found"
                }), 404)
            return jsonify(access_point.json())

        if page and limit:
            pagination = query.order_by(DefAccessPoint.creation_date.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            access_points = pagination.items
            results = [ap.json() for ap in access_points]
            return jsonify({
                "items": results,
                "page": pagination.page,
                "pages": pagination.pages,
                "total": pagination.total
            })
        else:
            access_points = query.order_by(DefAccessPoint.creation_date.desc()).all()
            results = [ap.json() for ap in access_points]
            return jsonify(results)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching access points", "error": str(e)}), 500)


@access_points_bp.route("/def_access_points_view", methods=["GET"])
@jwt_required()
def get_access_point_view():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        def_entitlement_id = request.args.get("def_entitlement_id", type=int)
        access_point_name = request.args.get("access_point_name", type=str)
        unlinked = request.args.get("unlinked", type=str)
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)

        query = DefAccessPointsV.query

        
        if access_point_name:
            query = query.filter(DefAccessPointsV.access_point_name.ilike(f"%{access_point_name}%"))

        if unlinked and unlinked.lower() == "true":
            query = query.filter(DefAccessPointsV.def_entitlement_id.is_(None))

        # Fetch single access point
        if def_access_point_id:
            access_point = query.filter_by(def_access_point_id=def_access_point_id).first()
            if not access_point:
                return make_response(jsonify({
                    "message": "Access point not found"
                }), 404)
            return jsonify(access_point.json())


        if def_entitlement_id:
            entitlement = query.filter_by(def_entitlement_id=def_entitlement_id).all()
            if not entitlement:
                return make_response(jsonify({
                    "message":  "No access point found"
                }), 404)
            results = [ent.json() for ent in entitlement]
            return jsonify(results)

        if page and limit:
            pagination = query.order_by(DefAccessPointsV.creation_date.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            access_points = pagination.items
            results = [ap.json() for ap in access_points]
            return jsonify({
                "items": results,
                "page": pagination.page,
                "pages": pagination.pages,
                "total": pagination.total
            })
        else:
            access_points = query.order_by(DefAccessPointsV.creation_date.desc()).all()
            results = [ap.json() for ap in access_points]
            return jsonify(results)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching access points", "error": str(e)}), 500)
    


@access_points_bp.route("/def_access_points", methods=["PUT"])
@jwt_required()
def update_access_point():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        if not def_access_point_id:
            return make_response(jsonify({"message": "def_access_point_id is required"}), 400)

        data = request.get_json() or {}
        user_id = get_jwt_identity()

        ap = db.session.query(DefAccessPoint).filter_by(def_access_point_id=def_access_point_id).first()
        if not ap:
            return make_response(jsonify({
                "message": "Access point not found"
            }), 404)


        def_data_source_id = data.get("def_data_source_id")
        if def_data_source_id is not None:
            data_source = db.session.query(DefDataSource).filter_by(def_data_source_id=def_data_source_id).first()
            if not data_source:
                return make_response(jsonify({
                    "message": "Invalid data source ID",
                    "error": "Data source not found"
                }), 400)

        def_entitlement_id = data.get("def_entitlement_id")


        ap.access_point_name = data.get("access_point_name", ap.access_point_name)
        ap.description = data.get("description", ap.description)
        ap.platform = data.get("platform", ap.platform)
        ap.access_point_type = data.get("access_point_type", ap.access_point_type)
        ap.access_control = data.get("access_control", ap.access_control)
        ap.change_control = data.get("change_control", ap.change_control)
        ap.audit = data.get("audit", ap.audit)
        ap.def_data_source_id = def_data_source_id if def_data_source_id is not None else ap.def_data_source_id
        ap.last_updated_by = user_id
        ap.last_update_date = datetime.utcnow()

        if def_entitlement_id is not None:
            entitlement_element = DefAccessEntitlementElement.query.filter_by(
                def_access_point_id=def_access_point_id
            ).first()  

            if entitlement_element:
                entitlement_element.def_entitlement_id = def_entitlement_id
                entitlement_element.last_updated_by = user_id
                entitlement_element.last_update_date = datetime.utcnow()



        db.session.commit()

        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error editing access point","error": str(e)}), 500)




@access_points_bp.route("/def_access_points", methods=["DELETE"])
@jwt_required()
def delete_access_point():
    try:
        # Get the access point ID from query params
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        if not def_access_point_id:
            return make_response(jsonify({"message": "def_access_point_id is required"}), 400)

        # Fetch the access point
        access_point = db.session.get(DefAccessPoint, def_access_point_id)
        if not access_point:
            return make_response(jsonify({
                "message": f"Access point with id {def_access_point_id} not found"
            }), 404)

        # Delete the access point
        db.session.delete(access_point)
        db.session.commit()

        return jsonify({"message": "Deleted successfully"})

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Error deleting access point",
            "error": str(e)
        }), 500)


