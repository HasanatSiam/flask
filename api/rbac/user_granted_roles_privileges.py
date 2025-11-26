from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required
from utils.auth import role_required
from executors.models import DefUserGrantedRolesPrivilegesV
from . import rbac_bp

@rbac_bp.route('/def_user_granted_roles_privileges', methods=['GET'])
@jwt_required()
def get_user_granted_roles_privileges():
    try:
        # Base query
        query = DefUserGrantedRolesPrivilegesV.query

        # Search filters
        user_name = request.args.get('user_name')
        tenant_id = request.args.get('tenant_id', type=int)
        user_id = request.args.get('user_id', type=int)

        if user_name:
            query = query.filter(DefUserGrantedRolesPrivilegesV.user_name.ilike(f'%{user_name}%'))
        
        if tenant_id:
            query = query.filter_by(tenant_id=tenant_id)

        if user_id:
            query = query.filter_by(user_id=user_id)
            result = query.first()
            return make_response(jsonify({
                "result": result.json() if result else {}
            }), 200)

        # Pagination
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [item.json() for item in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)
        
        # Return all if no pagination
        results = query.all()
        # return make_response(jsonify(result=[item.json() for item in results]), 200)
        return make_response(jsonify({
            "result": [item.json() for item in results]
        }), 200)
        

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching user granted roles and privileges"
        }), 500)
