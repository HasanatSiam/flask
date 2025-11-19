
from flask_jwt_extended import get_jwt_identity
from functools import wraps
from flask import request, jsonify, make_response 



def role_required():

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                from executors.models import (
                    DefUserGrantedRole,
                    DefApiEndpointRole,
                    DefApiEndpoint,
                    DefUserGrantedPrivilege

                )
                
                current_user_id = get_jwt_identity()
                if not current_user_id:
                    return jsonify({"message": "Authentication required"}), 401


                #  Extract route pattern
                rule = request.url_rule.rule        #  "/def_users/<int:user_id>/<status>"
                method = request.method             #  "GET"

                parts = rule.strip("/").split("/")

                api_endpoint = "/" + parts[0]       # "/def_users"

                parameter1 = None
                parameter2 = None

                if len(parts) > 1:
                    p1 = parts[1]
                    if p1.startswith("<") and p1.endswith(">"):
                        parameter1 = p1[1:-1].split(":")[-1]   # remove int: or string: type


                if len(parts) > 2:
                    p2 = parts[2]
                    if p2.startswith("<") and p2.endswith(">"):
                        parameter2 = p2[1:-1].split(":")[-1]



                #  Fetch allowed roles for this user
                user_roles = DefUserGrantedRole.query.filter_by(user_id=current_user_id).all()
                role_ids = [ur.role_id for ur in user_roles]

                if not role_ids:
                    return jsonify({"message": "No roles assigned"}), 403

                allowed_mappings = DefApiEndpointRole.query.filter(
                    DefApiEndpointRole.role_id.in_(role_ids)
                ).all()

                allowed_api_endpoint_ids = [m.api_endpoint_id for m in allowed_mappings]

                if not allowed_api_endpoint_ids:
                    return jsonify({"message": "User has no API access roles"}), 403


                #  Match the stored API endpoint rule in DB
                endpoint = DefApiEndpoint.query.filter(
                    DefApiEndpoint.api_endpoint_id.in_(allowed_api_endpoint_ids),
                    DefApiEndpoint.api_endpoint == api_endpoint,
                    DefApiEndpoint.method == method,
                    DefApiEndpoint.parameter1 == parameter1,
                    DefApiEndpoint.parameter2 == parameter2
                ).first()

                if not endpoint:
                    return jsonify({
                        "message": f"Access denied."
                    }), 403

                #  Check privilege
                user_privileges = DefUserGrantedPrivilege.query.filter_by(
                    user_id=current_user_id
                ).all()

                privilege_ids = [up.privilege_id for up in user_privileges]

                if endpoint.privilege_id not in privilege_ids:
                    return jsonify({"message": "Privilege denied"}), 403

                #  Access Granted
                return fn(*args, **kwargs)

            except Exception as e:
                return make_response(jsonify({"message": str(e)}), 500)

        return wrapper
    return decorator

