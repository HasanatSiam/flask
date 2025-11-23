
from flask_jwt_extended import get_jwt_identity
from functools import wraps
from flask import request, jsonify, make_response 
import hashlib
from Crypto.Cipher import AES
import base64



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
        return derived[:key_len], derived[key_len:key_len + iv_len]

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
        return derived[:key_len], derived[key_len:key_len + iv_len]

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

