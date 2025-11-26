from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_
from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefTenant,
    DefTenantEnterpriseSetup,
    DefTenantEnterpriseSetupV
)

from . import tenant_enterprise_bp

# Create enterprise setup
@tenant_enterprise_bp.route('/create_enterpriseV1/<int:tenant_id>', methods=['POST'])
@jwt_required()
def create_enterprise(tenant_id):
    try:
        data = request.get_json()
        tenant_id       = tenant_id
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']

        new_enterprise = DefTenantEnterpriseSetup(
            tenant_id=tenant_id,
            enterprise_name=enterprise_name,
            enterprise_type=enterprise_type,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by= get_jwt_identity(),
            last_update_date= datetime.utcnow()
        )

        db.session.add(new_enterprise)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

    except IntegrityError:
        return make_response(jsonify({"message": f"Enterprise setup already exists for tenant ID {tenant_id}."}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Failed to add enterprise setup.", "error": str(e)}), 500)

# Create or update enterprise setup
@tenant_enterprise_bp.route('/def_tenant_enterprise_setup', methods=['POST'])
@jwt_required()
def create_update_enterprise():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        data = request.get_json()
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']
        user_invitation_validity = data.get('user_invitation_validity', "1h")


        tenant_exists = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant_exists:
            return make_response(jsonify({"message": "Tenant does not exist"}), 400)
        
        existing_enterprise = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        existing_enterprise_name  = DefTenantEnterpriseSetup.query.filter_by(enterprise_name=enterprise_name).first()
        if existing_enterprise_name and (not existing_enterprise or existing_enterprise_name.tenant_id != tenant_id):
            return make_response(jsonify({"message": f"Enterprise name '{enterprise_name}' already exists."}), 409)





        if existing_enterprise:
            existing_enterprise.enterprise_name = enterprise_name
            existing_enterprise.enterprise_type = enterprise_type
            existing_enterprise.user_invitation_validity = user_invitation_validity
            existing_enterprise.last_updated_by = get_jwt_identity()
            existing_enterprise.last_update_date = datetime.utcnow()
            existing_enterprise.user_invitation_validity = user_invitation_validity
            message = "Edited successfully"

        else:
            new_enterprise = DefTenantEnterpriseSetup(
                tenant_id = tenant_id,
                enterprise_name = enterprise_name,
                enterprise_type = enterprise_type,
                user_invitation_validity = user_invitation_validity,
                created_by     = get_jwt_identity(),
                creation_date   = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )

            db.session.add(new_enterprise)
            message = "Added successfully"

        db.session.commit()
        return make_response(jsonify({"message": message, "result": new_enterprise.json() if not existing_enterprise else existing_enterprise.json()}), 200)

    except IntegrityError:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": "Integrity error"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": str(e)}), 500)



#Get all enterprise setups
@tenant_enterprise_bp.route('/get_enterprises', methods=['GET'])
@jwt_required()
def get_enterprises():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)

@tenant_enterprise_bp.route('/get_enterprises/v1', methods=['GET'])
def get_enterprises_v1():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)





# Update enterprise setup
@tenant_enterprise_bp.route('/update_enterprise/<int:tenant_id>', methods=['PUT'])
@jwt_required()
def update_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            data = request.get_json()
            setup.enterprise_name = data.get('enterprise_name', setup.enterprise_name)
            setup.enterprise_type = data.get('enterprise_type', setup.enterprise_type)
            setup.user_invitation_validity = data.get('user_invitation_validity', setup.user_invitation_validity)
            setup.last_updated_by = get_jwt_identity()
            setup.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error Editing enterprise setup", "error": str(e)}), 500)


# Delete enterprise setup
@tenant_enterprise_bp.route('/def_tenant_enterprise_setup', methods=['DELETE'])
@jwt_required()
def delete_enterprise():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            db.session.delete(setup)
            db.session.commit()
            return make_response(jsonify({"message": "Enterprise setup deleted successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting enterprise setup", "error": str(e)}), 500)

 

 



@tenant_enterprise_bp.route('/def_tenant_enterprise_setup', methods=['GET'])
@jwt_required()
def get_tenant_enterprise_setup():
    try:
        # Base query using the View model
        query = db.session.query(DefTenantEnterpriseSetupV)

        # Search filter
        enterprise_name = request.args.get('enterprise_name', '').strip()
        if enterprise_name:
            query = query.filter(DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{enterprise_name}%'))

        # Filter by tenant_id
        tenant_id = request.args.get('tenant_id', type=int)
        if tenant_id:
            query = query.filter(DefTenantEnterpriseSetupV.tenant_id == tenant_id)
            result = query.first()
            return make_response(jsonify({
                "result": result.json() if result else {}
            }), 200)

        # Ordering
        query = query.order_by(DefTenantEnterpriseSetupV.tenant_id.desc())

        # Pagination
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [row.json() for row in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)
        
        # Return all if no pagination
        results = query.all()
        return make_response(jsonify({"result": [row.json() for row in results]}), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching enterprises", "error": str(e)}), 500)


