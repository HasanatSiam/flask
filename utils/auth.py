from flask_jwt_extended import get_jwt_identity
from functools import wraps
from flask import request, jsonify, make_response
import hashlib
from Crypto.Cipher import AES
import base64
import logging

from executors.models import (
    DefUserGrantedRole,
    DefApiEndpointRole,
    DefApiEndpoint,
    DefUserGrantedPrivilege,
    DefUser,
)
from executors.extensions import cache, db

logger = logging.getLogger(__name__)


def role_required():

    def decorator(fn):

        @wraps(fn)
        def wrapper(*args, **kwargs):

            try:

                current_user_id = get_jwt_identity()

                if not current_user_id:
                    return jsonify({
                        "message": "Authentication required"
                    }), 401

                # =====================================================
                # User Info
                # =====================================================

                user = db.session.get(
                    DefUser,
                    int(current_user_id)
                )

                user_tenant_id = user.tenant_id if user else None

                # =====================================================
                # Extract Route Information
                # =====================================================

                rule = request.url_rule.rule
                method = request.method

                parts = rule.strip("/").split("/")

                base_parts = []
                path_param_names = []

                for part in parts:

                    if part.startswith("<") and part.endswith(">"):

                        path_param_names.append(
                            part[1:-1].split(":")[-1]
                        )

                    else:
                        base_parts.append(part)

                api_endpoint = "/" + "/".join(base_parts)

                # =====================================================
                # User Roles
                # =====================================================

                user_roles = DefUserGrantedRole.query.filter_by(
                    user_id=current_user_id
                ).all()

                role_ids = [
                    role.role_id
                    for role in user_roles
                ]

                if not role_ids:
                    return jsonify({
                        "message": "No roles assigned"
                    }), 403

                # =====================================================
                # Allowed Endpoint IDs
                # =====================================================

                allowed_mappings = DefApiEndpointRole.query.filter(
                    DefApiEndpointRole.role_id.in_(role_ids)
                ).all()

                allowed_api_endpoint_ids = [
                    mapping.api_endpoint_id
                    for mapping in allowed_mappings
                ]

                if not allowed_api_endpoint_ids:
                    return jsonify({
                        "message": "User has no API access roles"
                    }), 403

                # =====================================================
                # RBAC Cache
                # =====================================================

                cache_key = f"rbac:user:{current_user_id}"

                user_rbac = cache.get(cache_key)

                if user_rbac is None:

                    user_roles_fresh = DefUserGrantedRole.query.filter_by(
                        user_id=current_user_id
                    ).all()

                    user_privileges_fresh = DefUserGrantedPrivilege.query.filter_by(
                        user_id=current_user_id
                    ).all()

                    user_rbac = {
                        "role_ids": [
                            r.role_id
                            for r in user_roles_fresh
                        ],
                        "privilege_ids": [
                            p.privilege_id
                            for p in user_privileges_fresh
                        ]
                    }

                    cache.set(cache_key, user_rbac)

                privilege_ids = user_rbac["privilege_ids"]

                # =====================================================
                # Find Endpoint
                # =====================================================

                endpoints = DefApiEndpoint.query.filter(
                    DefApiEndpoint.api_endpoint_id.in_(
                        allowed_api_endpoint_ids
                    ),
                    DefApiEndpoint.api_endpoint == api_endpoint,
                    DefApiEndpoint.method == method
                ).all()

                if not endpoints:
                    return jsonify({
                        "message": "Access denied"
                    }), 403

                # =====================================================
                # Validate Parameters & Find Exact Match
                # =====================================================

                actual_path_params = sorted(path_param_names)

                endpoint = None
                stored_required_query_params = []
                last_expected_path_params = []

                for ep in endpoints:
                    ep_params = ep.parameters or []
                    ep_path_params = sorted([
                        p["name"] for p in ep_params if p.get("location") == "path"
                    ])

                    last_expected_path_params = ep_path_params

                    if ep_path_params == actual_path_params:
                        endpoint = ep
                        stored_required_query_params = [
                            p["name"] for p in ep_params 
                            if p.get("location") == "query" and p.get("required") is True
                        ]
                        break

                # -----------------------------------------------------
                # Path Parameter Validation
                # -----------------------------------------------------

                if not endpoint:
                    return jsonify({
                        "message": "Path parameter mismatch",
                        "expected": last_expected_path_params,
                        "received": actual_path_params
                    }), 403

                # -----------------------------------------------------
                # Required Query Parameter Validation
                # -----------------------------------------------------

                for query_param in stored_required_query_params:

                    if request.args.get(query_param) is None:

                        return jsonify({
                            "message":
                                f"Missing required query parameter '{query_param}'"
                        }), 400

                # =====================================================
                # Privilege Validation
                # =====================================================

                if (
                    endpoint.privilege_id
                    and endpoint.privilege_id not in privilege_ids
                ):

                    return jsonify({
                        "message": "Privilege denied"
                    }), 403

                # =====================================================
                # Execute Endpoint
                # =====================================================

                response = fn(*args, **kwargs)

                flask_response = make_response(response)

                endpoint_id = endpoint.api_endpoint_id

                # =====================================================
                # Webhook Trigger
                # =====================================================

                try:

                    status_code = flask_response.status_code

                    if 200 <= status_code < 300:

                        from utils.webhook_service import fire

                        if user_tenant_id and endpoint_id:

                            payload = (
                                flask_response.get_json(
                                    silent=True
                                ) or {}
                            )

                            fire(
                                api_endpoint_id=endpoint_id,
                                payload=payload,
                                tenant_id=user_tenant_id
                            )

                except Exception as webhook_error:

                    logger.error(
                        f"[WebhookV2] Trigger error: {webhook_error}"
                    )

                return flask_response

            except Exception as e:

                logger.exception("RBAC Error")

                return make_response(
                    jsonify({
                        "message": str(e)
                    }),
                    500
                )

        return wrapper

    return decorator

def encrypt(value, passphrase):
    value_bytes = value.encode("utf-8")
    passphrase = passphrase.encode("utf-8")

    salt = b"12345678"  # fixed salt for frontend decryption

    def evp_bytes_to_key(password, salt, key_len=32, iv_len=16):
        dt = b""
        derived = b""
        while len(derived) < key_len + iv_len:
            dt = hashlib.md5(dt + password + salt).digest()
            derived += dt
        return derived[:key_len], derived[key_len : key_len + iv_len]

    key, iv = evp_bytes_to_key(passphrase, salt)

    cipher = AES.new(key, AES.MODE_CBC, iv)

    pad_len = 16 - (len(value_bytes) % 16)
    padded = value_bytes + bytes([pad_len]) * pad_len

    encrypted = cipher.encrypt(padded)
    openssl_bytes = b"Salted__" + salt + encrypted

    return base64.urlsafe_b64encode(openssl_bytes).decode()

 
def decrypt(encrypted_value, passphrase):
    # URL-decode and Base64-decode
    encrypted_bytes = base64.urlsafe_b64decode(encrypted_value)

    if encrypted_bytes[:8] != b"Salted__":
        raise ValueError("Invalid encrypted data")

    salt = encrypted_bytes[8:16]  # should match the salt used in encrypt()
    encrypted_data = encrypted_bytes[16:]

    passphrase = passphrase.encode("utf-8")

    # Key derivation (same as in encrypt)
    def evp_bytes_to_key(password, salt, key_len=32, iv_len=16):
        dt = b""
        derived = b""
        while len(derived) < key_len + iv_len:
            dt = hashlib.md5(dt + password + salt).digest()
            derived += dt
        return derived[:key_len], derived[key_len : key_len + iv_len]

    key, iv = evp_bytes_to_key(passphrase, salt)

    # AES decryption
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_padded = cipher.decrypt(encrypted_data)

    # Remove PKCS#7 padding
    pad_len = decrypted_padded[-1]
    if pad_len < 1 or pad_len > 16:
        raise ValueError("Invalid padding")
    decrypted = decrypted_padded[:-pad_len]

    return decrypted.decode("utf-8")
