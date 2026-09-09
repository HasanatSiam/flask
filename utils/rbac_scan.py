"""
utils/rbac_scan.py

Helpers to compare live Flask routes against the def_api_endpoints table.
"""

import inspect
import re

from flask import current_app

from executors.models import DefApiEndpoint, DefPrivilege


SKIP_ENDPOINTS = {"static"}
SKIP_METHODS = {"HEAD", "OPTIONS"}


def _clean_endpoint_path(rule: str) -> str:
    """
    Strip every /<type:name> segment from a Flask URL rule.

    /defusers/<int:page>/<int:limit> → /defusers
    """
    cleaned = re.sub(r'/<[^>]+>', '', rule)
    if cleaned != '/':
        cleaned = cleaned.rstrip('/')
    return cleaned or '/'


def _flask_type_to_str(flask_type):
    return {"int": "integer", "float": "float", "uuid": "uuid", "path": "string"}.get(
        flask_type or "", "string"
    )


def _python_type_to_str(python_type):
    return {"int": "integer", "float": "float", "bool": "boolean", "str": "string"}.get(
        python_type or "", "string"
    )


def _extract_parameters(rule, app) -> list:
    """
    Extract path parameters from the URL rule and query parameters from
    the handler source (best-effort), in the def_api_endpoints format.
    """
    params = []

    # 1. Path parameters from URL rule
    for match in re.finditer(r'<(?:(\w+):)?(\w+)>', str(rule.rule)):
        params.append({
            "name":     match.group(2),
            "type":     _flask_type_to_str(match.group(1)),
            "required": True,
            "location": "path",
        })

    # 2. Query parameters from handler source (best-effort)
    view_func = app.view_functions.get(rule.endpoint)
    if view_func:
        try:
            source  = inspect.getsource(view_func)
            pattern = re.compile(
                r"request\.args\.get\(\s*['\"]([^'\"]+)['\"]\s*"
                r"(?:[^)]*?type\s*=\s*(\w+))?"
            )
            seen_qp = set()
            for m in pattern.finditer(source):
                name = m.group(1)
                if name in seen_qp:
                    continue
                seen_qp.add(name)
                params.append({
                    "name":     name,
                    "type":     _python_type_to_str(m.group(2)),
                    "required": False,
                    "location": "query",
                })
        except (OSError, TypeError):
            pass

    return params


METHOD_ACTIONS = {
    "GET": "Get",
    "POST": "Create",
    "PUT": "Update",
    "PATCH": "Update",
    "DELETE": "Delete",
}

# Maps HTTP method -> privilege name used to look up privilege_id
METHOD_PRIVILEGE_NAMES = {
    "GET":    "query",
    "POST":   "create",
    "PUT":    "update",
    "PATCH":  "update",
    "DELETE": "delete",
}

STANDALONE_ACTIONS = {
    "login", "logout", "signin", "signout", "signup", "register",
    "refresh", "verify", "validate", "authenticate", "reset",
    "toggle", "run", "execute", "stream", "poll", "subscribe",
    "import", "export", "download", "upload", "send", "notify", "invite",
}

CRUD_VERBS = {
    "get", "fetch", "list", "read", "find", "retrieve",
    "create", "add", "new", "insert",
    "update", "edit", "modify", "change", "patch",
    "delete", "remove", "destroy",
    "upsert", "manage",
}


def _generate_api_name(rule, method: str, app) -> str:
    """
    Generate a human-readable API name based on HTTP method and view function.

    - For standalone actions (login, logout, refresh, verify, etc.), no method prefix is added.
    - For CRUD operations, maps HTTP methods to actions:
        GET    -> Get
        POST   -> Create
        PUT    -> Update
        DELETE -> Delete
    - Replaces generic/CRUD prefixes like 'upsert_' or 'manage_' with the appropriate method action.
    """
    view_func = app.view_functions.get(rule.endpoint)
    raw_name = None

    if view_func:
        try:
            unwrapped = inspect.unwrap(view_func)
            fn_name = getattr(unwrapped, '__name__', None) or getattr(view_func, '__name__', None)
            if fn_name and fn_name not in ('wrapper', 'decorator', 'decorated_function'):
                raw_name = fn_name
        except Exception:
            pass

    if not raw_name and rule.endpoint:
        endpoint_name = rule.endpoint.split('.')[-1]
        if endpoint_name not in ('wrapper', 'decorator', 'decorated_function'):
            raw_name = endpoint_name

    action = METHOD_ACTIONS.get(method.upper(), method.strip().title())

    if raw_name:
        words = raw_name.replace('_', ' ').strip().split()
        if words:
            first_word = words[0].lower()

            # Compound standalone words: log_in / log_out / sign_in / sign_out
            if first_word in {'log', 'sign'} and len(words) > 1 and words[1].lower() in {'in', 'out'}:
                return f"{words[0].title()}{words[1].title()}"

            # Standalone actions (e.g. login, logout, refresh_token, verify_token)
            if first_word in STANDALONE_ACTIONS:
                return ' '.join(words).title()

            # CRUD verbs (e.g. upsert_profile_picture -> Create Profile Picture / Update Profile Picture)
            if first_word in CRUD_VERBS:
                base_name = ' '.join(words[1:]).title()
                return f"{action} {base_name}".strip() if base_name else action

            # General function without leading verb (e.g. user_details -> Get User Details / Update User Details)
            base_name = ' '.join(words).title()
            return f"{action} {base_name}".strip()

    # Fallback: Method action + Cleaned Path
    cleaned = _clean_endpoint_path(str(rule.rule)).strip('/')
    segments = [s for s in re.split(r'[/_-]+', cleaned) if s]
    if segments and segments[-1].lower() in STANDALONE_ACTIONS:
        return segments[-1].title()

    base = ' '.join(segments).title()
    return f"{action} {base}".strip() if base else action


def _build_method_privilege_map() -> dict:
    """
    Query DefPrivilege once and return a mapping of
    HTTP method -> privilege_id based on METHOD_PRIVILEGE_NAMES.
    """
    privilege_map = {}
    privilege_name_to_id = {
        row.privilege_name.lower(): row.privilege_id
        for row in DefPrivilege.query.all()
    }
    for method, priv_name in METHOD_PRIVILEGE_NAMES.items():
        privilege_map[method] = privilege_name_to_id.get(priv_name)
    return privilege_map


def scan_unregistered_endpoints() -> dict:
    """
    Compare live Flask routes against the def_api_endpoints table and
    return the (path, method) pairs that are not registered yet, with
    the parameters needed for registration.
    """
    live = {}
    for rule in current_app.url_map.iter_rules():
        if rule.endpoint in SKIP_ENDPOINTS:
            continue
        api_endpoint = _clean_endpoint_path(str(rule.rule))
        for method in rule.methods - SKIP_METHODS:
            live.setdefault((api_endpoint, method), rule)

    registered = {
        (row.api_endpoint, row.method)
        for row in DefApiEndpoint.query.all()
    }
    unregistered = sorted(key for key in live if key not in registered)

    method_privilege_map = _build_method_privilege_map()

    return {
        "result": [
            {
                "api_endpoint": api_endpoint,
                "api_name": _generate_api_name(live[(api_endpoint, method)], method, current_app),
                "method": method,
                "privilege_id": method_privilege_map.get(method),
                "parameters": _extract_parameters(live[(api_endpoint, method)], current_app)
            }
            for api_endpoint, method in unregistered
        ],
        "total": len(unregistered)
    }
