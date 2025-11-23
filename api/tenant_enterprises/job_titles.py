from datetime import datetime
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from executors.models import (
    DefTenant,
    DefJobTitle
)


from . import tenant_enterprise_bp

@tenant_enterprise_bp.route('/job_titles', methods=['POST'])
@jwt_required()
def create_job_title():
    try:
        data = request.get_json()
        job_title_name = data.get('job_title_name')
        tenant_id = data.get('tenant_id')

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant:
            return jsonify({"message": "Invalid tenant_id. Tenant not found."}), 404

        existing_title = DefJobTitle.query.filter_by(job_title_name=job_title_name, tenant_id=tenant_id).first()
        if existing_title:
            return jsonify({"message": f"'{job_title_name}' already exists for this tenant."}), 409

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
        return jsonify({
            "message": "Added successfully",
            "job_title_id": new_title.job_title_id
        }), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Failed to create job title", "error": str(e)}), 500


@tenant_enterprise_bp.route('/job_titles', methods=['GET'])
@jwt_required()
def get_job_titles():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        tenant_id = request.args.get('tenant_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int, default=10)

        # If job_title_id is provided → return single object
        if job_title_id:
            job = DefJobTitle.query.filter(DefJobTitle.job_title_id == job_title_id).first()
            if not job:
                return make_response(jsonify({"message": "Job title not found"}), 404)
            return make_response(jsonify(job.json()), 200)

        # Base query
        query = DefJobTitle.query

        # Filter by tenant if provided
        if tenant_id:
            query = query.filter(DefJobTitle.tenant_id == tenant_id)

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
def update_job_title():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        if not job_title_id:
            return make_response(jsonify({"message": "Missing query parameter: job_title_id"}), 400)

        title = db.session.get(DefJobTitle, job_title_id)
        if not title:
            return make_response(jsonify({"message": "Job title not found"}), 404)

        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "Missing JSON body"}), 400)

        new_job_title_name = data.get('job_title_name', title.job_title_name)
        new_tenant_id = data.get('tenant_id', title.tenant_id)

        # Duplicate check — only if name or tenant is changing
        existing_title = (
            DefJobTitle.query.filter_by(job_title_name=new_job_title_name, tenant_id=new_tenant_id)
            .filter(DefJobTitle.job_title_id != job_title_id)  # exclude current record
            .first()
        )
        if existing_title:
            return jsonify({
                "message": f"'{new_job_title_name}' already exists for this tenant."
            }), 409


        title.job_title_name   = new_job_title_name
        title.tenant_id        = new_tenant_id
        title.last_updated_by  = get_jwt_identity()
        title.last_update_date = datetime.utcnow()
        db.session.commit()

        return jsonify({
            "message": "Edited successfully",
            "job_title_id": title.job_title_id
        }), 200

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to update job title",
            "error": str(e)
        }), 500)



@tenant_enterprise_bp.route('/job_titles', methods=['DELETE'])
@jwt_required()
def delete_job_title():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        if not job_title_id:
            return make_response(jsonify({"message": "Missing query parameter: job_title_id"}), 400)

        title = db.session.get(DefJobTitle, job_title_id)
        if not title:
            return make_response(jsonify({"message": "Job title not found"}), 404)

        db.session.delete(title)
        db.session.commit()
        return jsonify({"message": "Deleted successfully"}), 200

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to delete job title",
            "error": str(e)
        }), 500)

