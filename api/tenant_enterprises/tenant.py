from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefTenant,
    DefTenantEnterpriseSetup,
    DefJobTitle
)


from . import tenant_enterprise_bp
from utils.auth import role_required






# Create a tenant
@tenant_enterprise_bp.route('/def_tenants', methods=['POST'])
@jwt_required()
def create_tenant():
    try:
       data = request.get_json()
    #    tenant_id   = generate_tenant_id()  # Call the function to get the result
       tenant_name = data['tenant_name']
       existing_name = DefTenant.query.filter_by(tenant_name=tenant_name).first()
       if existing_name:
            return make_response(jsonify({"message": "Tenant name already exists"}), 400)


       new_tenant  = DefTenant(
            tenant_name = tenant_name,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
           )
       db.session.add(new_tenant)
       db.session.commit()
       return make_response(jsonify({"message": "Added successfully"}), 201)
   
    except IntegrityError as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": "Tenant already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": str(e)}), 500)

       

# Get all tenants


@tenant_enterprise_bp.route('/def_tenants', methods=['GET'])
@jwt_required()
def get_tenants():
    try:
        tenants = DefTenant.query.order_by(
            DefTenant.tenant_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [tenant.json() for tenant in tenants]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error getting Tenants",
            "error": str(e)
        }), 500)


@tenant_enterprise_bp.route('/tenants/v1', methods=['GET'])
def get_tenants_v1():
    try:
        tenants = DefTenant.query.order_by(DefTenant.tenant_id.desc()).all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)



@tenant_enterprise_bp.route('/def_tenants/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_tenants(page, limit):
    try:
        query = DefTenant.query.order_by(DefTenant.tenant_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching tenants", "error": str(e)}), 500)


@tenant_enterprise_bp.route('/def_tenants/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_tenants(page, limit):
    try:
        search_query = request.args.get('tenant_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefTenant.query

        if search_query:
            query = query.filter(
                or_(
                    DefTenant.tenant_name.ilike(f'%{search_query}%'),
                    DefTenant.tenant_name.ilike(f'%{search_underscore}%'),
                    DefTenant.tenant_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefTenant.tenant_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching tenants", "error": str(e)}), 500)



@tenant_enterprise_bp.route('/tenants/<int:tenant_id>', methods=['GET'])
@jwt_required()
def get_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            return make_response(jsonify(tenant.json()),200)
        else:
            return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving tenant", "error": str(e)}), 500)


# Update a tenant
@tenant_enterprise_bp.route('/def_tenants', methods=['PUT'])
@jwt_required()
def update_tenant():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            data = request.get_json()
            tenant.tenant_name  = data['tenant_name']
            tenant.last_updated_by = get_jwt_identity()
            tenant.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Tenant", "error": str(e)}), 500)


# Delete a tenant
@tenant_enterprise_bp.route('/def_tenants', methods=['DELETE'])
@jwt_required()
def delete_tenant():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        user = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting tenant", "error": str(e)}), 500)
    
@tenant_enterprise_bp.route('/tenants/cascade_delete', methods=['DELETE'])
@jwt_required()
def delete_tenant_and_related():
    try:
        tenant_id = request.args.get('tenant_id', type=int)

        if not tenant_id:
            return jsonify({"message": "Missing required query parameter: tenant_id"}), 400

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant:
            return jsonify({"message": "Tenant not found"}), 404

        # Delete related records manually
        DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).delete()
        DefJobTitle.query.filter_by(tenant_id=tenant_id).delete()

        db.session.delete(tenant)
        db.session.commit()

        return jsonify({
            "message": f"Deleted successfully"
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "message": "Failed to delete tenant and related data",
            "error": str(e)
        }), 500

