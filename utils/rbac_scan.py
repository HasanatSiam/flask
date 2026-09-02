"""
utils/rbac_scan.py

Helpers to compare live Flask routes against the def_api_endpoints table.
"""

import inspect
import re

from flask import current_app

from executors.models import DefApiEndpoint


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

    return {
        "result": [
            {
                "api_endpoint": api_endpoint,
                "method": method,
                "parameters": _extract_parameters(live[(api_endpoint, method)], current_app)
            }
            for api_endpoint, method in unregistered
        ],
        "total": len(unregistered)
    }
