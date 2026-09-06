from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefTenant,
    DefJobTitle
)

from . import tenant_enterprise_bp
from utils.auth import role_required, is_superadmin, is_admin, get_user_tenant_id


@tenant_enterprise_bp.route('/job_titles', methods=['POST'])
@jwt_required()
@role_required()
def create_job_title():
    try:
        data = request.get_json(silent=True) or {}
        job_title_name = data.get('job_title_name')
        tenant_id = data.get('tenant_id')

        if not job_title_name:
            return make_response(jsonify({"message": "job_title_name is required"}), 400)

        # Role & tenant authorization check
        if is_superadmin():
            if not tenant_id:
                return make_response(jsonify({"message": "tenant_id is required for superadmin"}), 400)
        elif is_admin():
            user_tenant_id = get_user_tenant_id()
            if not user_tenant_id:
                return make_response(jsonify({"message": "User is not associated with any tenant"}), 403)
            if tenant_id and tenant_id != user_tenant_id:
                return make_response(jsonify({"message": "Access denied: You can only create job titles for your respective tenant"}), 403)
            tenant_id = user_tenant_id
        else:
            return make_response(jsonify({"message": "Access denied: Insufficient permissions"}), 403)

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant:
            return make_response(jsonify({"message": "Invalid tenant_id. Tenant not found."}), 404)

        existing_title = DefJobTitle.query.filter_by(job_title_name=job_title_name, tenant_id=tenant_id).first()
        if existing_title:
            return make_response(jsonify({"message": f"'{job_title_name}' already exists for this tenant."}), 409)

        new_title = DefJobTitle(
            job_title_name   = job_title_name,
            tenant_id        = tenant_id,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_title)
        db.session.commit()
        return make_response(jsonify({
            "message": "Added successfully",
            "job_title_id": new_title.job_title_id
        }), 201)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Failed to create job title", "error": str(e)}), 500)


@tenant_enterprise_bp.route('/job_titles', methods=['GET'])
@jwt_required()
@role_required()
def get_job_titles():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        tenant_id = request.args.get('tenant_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int, default=10)

        # Role & tenant authorization check
        if is_superadmin():
            query = DefJobTitle.query
            if tenant_id:
                query = query.filter(DefJobTitle.tenant_id == tenant_id)
        elif is_admin():
            user_tenant_id = get_user_tenant_id()
            if not user_tenant_id:
                return make_response(jsonify({"message": "User is not associated with any tenant"}), 403)
            if tenant_id and tenant_id != user_tenant_id:
                return make_response(jsonify({"message": "Access denied: You can only view job titles for your respective tenant"}), 403)
            query = DefJobTitle.query.filter(DefJobTitle.tenant_id == user_tenant_id)
        else:
            return make_response(jsonify({"message": "Access denied: Insufficient permissions"}), 403)

        # If job_title_id is provided → return single object
        if job_title_id:
            job = query.filter(DefJobTitle.job_title_id == job_title_id).first()
            if not job:
                return make_response(jsonify({"message": "Job title not found"}), 404)
            return make_response(jsonify(job.json()), 200)

        # Always order by id descending
        query = query.order_by(DefJobTitle.job_title_id.desc())

        # Pagination if page parameter provided
        if page:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "items": [item.json() for item in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # No pagination → return all matching items
        items = query.all()
        if not items:
            return make_response(jsonify({"message": "No job titles found"}), 404)
        return make_response(jsonify([item.json() for item in items]), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to retrieve job titles",
            "error": str(e)
        }), 500)


@tenant_enterprise_bp.route('/job_titles', methods=['PUT'])
@jwt_required()
@role_required()
def update_job_title():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        if not job_title_id:
            return make_response(jsonify({"message": "Missing query parameter: job_title_id"}), 400)

        title = db.session.get(DefJobTitle, job_title_id)
        if not title:
            return make_response(jsonify({"message": "Job title not found"}), 404)

        # Role & tenant authorization check
        if is_superadmin():
            pass
        elif is_admin():
            user_tenant_id = get_user_tenant_id()
            if not user_tenant_id or title.tenant_id != user_tenant_id:
                return make_response(jsonify({"message": "Access denied: You can only update job titles for your respective tenant"}), 403)
        else:
            return make_response(jsonify({"message": "Access denied: Insufficient permissions"}), 403)

        data = request.get_json(silent=True)
        if not data:
            return make_response(jsonify({"message": "Missing JSON body"}), 400)

        new_job_title_name = data.get('job_title_name', title.job_title_name)

        if is_superadmin():
            new_tenant_id = data.get('tenant_id', title.tenant_id)
        else:
            new_tenant_id = title.tenant_id
            if 'tenant_id' in data and data['tenant_id'] != user_tenant_id:
                return make_response(jsonify({"message": "Access denied: Cannot reassign job title to a different tenant"}), 403)

        if new_tenant_id != title.tenant_id:
            tenant = DefTenant.query.filter_by(tenant_id=new_tenant_id).first()
            if not tenant:
                return make_response(jsonify({"message": "Invalid tenant_id. Tenant not found."}), 404)

        # Duplicate check — only if name or tenant is changing
        existing_title = (
            DefJobTitle.query.filter_by(job_title_name=new_job_title_name, tenant_id=new_tenant_id)
            .filter(DefJobTitle.job_title_id != job_title_id)  # exclude current record
            .first()
        )
        if existing_title:
            return make_response(jsonify({
                "message": f"'{new_job_title_name}' already exists for this tenant."
            }), 409)

        title.job_title_name   = new_job_title_name
        title.tenant_id        = new_tenant_id
        title.last_updated_by  = get_jwt_identity()
        title.last_update_date = datetime.utcnow()
        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "job_title_id": title.job_title_id
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to update job title",
            "error": str(e)
        }), 500)


@tenant_enterprise_bp.route('/job_titles', methods=['DELETE'])
@jwt_required()
@role_required()
def delete_job_title():
    try:
        data = request.get_json(silent=True) or {}
        job_title_ids = data.get('job_title_ids')

        # Support single job_title_id via query param or body as fallback
        if not job_title_ids:
            single_id = request.args.get('job_title_id', type=int) or data.get('job_title_id')
            if single_id:
                job_title_ids = [single_id]

        if not job_title_ids:
            return make_response(jsonify({
                "message": "Request body with 'job_title_ids' (list) or query parameter 'job_title_id' is required"
            }), 400)

        if not isinstance(job_title_ids, list):
            return make_response(jsonify({'message': 'job_title_ids must be a list'}), 400)

        titles = DefJobTitle.query.filter(DefJobTitle.job_title_id.in_(job_title_ids)).all()

        if not titles:
            return make_response(jsonify({'message': 'No job titles found for provided IDs'}), 404)

        # Role & tenant authorization check
        if is_superadmin():
            pass
        elif is_admin():
            user_tenant_id = get_user_tenant_id()
            if not user_tenant_id or any(t.tenant_id != user_tenant_id for t in titles):
                return make_response(jsonify({"message": "Access denied: You can only delete job titles for your respective tenant"}), 403)
        else:
            return make_response(jsonify({"message": "Access denied: Insufficient permissions"}), 403)

        for title in titles:
            db.session.delete(title)

        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to delete job titles",
            "error": str(e)
        }), 500)


