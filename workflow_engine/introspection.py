"""
Workflow Introspection Utilities

Helper functions to analyze workflow nodes/tasks and determine
expected inputs/outputs by:
1. Querying DB-defined parameters (DefAsyncTaskParam)
2. Introspecting Python scripts for globals().get() patterns (inputs)
3. Introspecting Python scripts for result = {...} patterns (outputs)
"""

import os
import re
from executors.models import DefAsyncTaskParam


def introspect_inputs(script_path):
    """
    Return list of global keys read via globals().get('key') in script.
    Only returns parameters WITHOUT default values (truly required inputs).
    
    - globals().get('key')           -> REQUIRED (returned)
    - globals().get('key', default)  -> OPTIONAL (skipped)
    """
    keys = []
    if not script_path or not os.path.isfile(script_path):
        return keys
    try:
        content = open(script_path, 'r', encoding='utf-8').read()
        # Pattern to match globals().get('key') or globals().get('key', default)
        # Capture the key and check if there's a comma (meaning default exists)
        for m in re.finditer(r"globals\(\)\.get\(\s*['\"](?P<key>[\w_]+)['\"](?P<has_default>\s*,)?", content):
            # Only include if NO default value (no comma after the key)
            if not m.group('has_default'):
                keys.append(m.group('key'))
    except Exception:
        pass
    return list(dict.fromkeys(keys))  # preserve order, unique


def introspect_outputs(script_path):
    """
    Heuristic: find output keys from:
    1. Top-level result = { 'k': ... } 
    2. return { 'k': ... } statements inside functions
    
    Filters out error-related keys (error, err, exception, message) since these
    are typically error responses, not useful data for chaining.
    """
    # Keys to exclude from outputs (error responses, not useful for chaining)
    EXCLUDED_KEYS = {'error', 'err', 'exception', 'message', 'msg'}
    
    keys = []
    if not script_path or not os.path.isfile(script_path):
        return keys
    try:
        content = open(script_path, 'r', encoding='utf-8').read()
        
        # Pattern 1: result = { ... }
        m = re.search(r"\bresult\s*=\s*\{([^}]*)\}", content, re.S)
        if m:
            body = m.group(1)
            for k in re.finditer(r"['\"](?P<key>[\w_]+)['\"]\s*:", body):
                key = k.group('key')
                if key.lower() not in EXCLUDED_KEYS:
                    keys.append(key)
        
        # Pattern 2: return { ... } - find all return dicts
        for m in re.finditer(r"\breturn\s*\{([^}]*)\}", content, re.S):
            body = m.group(1)
            for k in re.finditer(r"['\"](?P<key>[\w_]+)['\"]\s*:", body):
                key = k.group('key')
                if key.lower() not in EXCLUDED_KEYS:
                    keys.append(key)
                
    except Exception:
        pass
    return list(dict.fromkeys(keys))  # unique, preserve order



def batch_db_defined_inputs(task_names):
    """
    Get parameter names for multiple tasks in a single DB query.
    Returns: { 'task_name': ['param1', 'param2'], ... }
    """
    if not task_names:
        return {}
    
    # Filter out None and duplicates
    unique_names = list(set(filter(None, task_names)))
    if not unique_names:
        return {}

    try:
        rows = DefAsyncTaskParam.query.filter(DefAsyncTaskParam.task_name.in_(unique_names)).all()
        result = {}
        for row in rows:
            if row.task_name not in result:
                result[row.task_name] = []
            result[row.task_name].append(row.parameter_name)
        return result
    except Exception:
        return {}


def build_predecessors(nodes, edges):
    """Build a dict mapping node_id -> list of predecessor node_ids."""
    preds = {n['id']: [] for n in nodes}
    for e in edges or []:
        src = e.get('source')
        tgt = e.get('target')
        if src and tgt and tgt in preds:
            preds[tgt].append(src)
    return preds
