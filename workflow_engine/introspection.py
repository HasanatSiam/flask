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
    
    Returns: [{'name': 'key'}, ...]
    """
    keys = []
    if not script_path or not os.path.isfile(script_path):
        return keys
    try:
        content = open(script_path, 'r', encoding='utf-8').read()
        # Pattern to match globals().get('key') or globals().get('key', default)
        for m in re.finditer(r"globals\(\)\.get\(\s*['\"](?P<key>[\w_]+)['\"](?P<has_default>\s*,)?", content):
            if not m.group('has_default'):
                keys.append(m.group('key'))
    except Exception:
        pass
    
    # Unique names while preserving order
    unique_names = list(dict.fromkeys(keys))
    return [{"name": name} for name in unique_names]


def introspect_outputs(script_path):
    """
    Heuristic: find output keys from:
    1. Top-level result = { 'k': ... } 
    2. return { 'k': ... } statements inside functions
    3. return json.dumps({ 'k': ... }) patterns

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

        # Key extractor: allows word chars, underscores, and spaces (e.g. "Employee Name")
        KEY_PATTERN = r"""['"]([\w_ ]+)['"]\s*:"""

        def _extract_keys(body):
            result_keys = []
            for k in re.finditer(KEY_PATTERN, body):
                key = k.group(1).strip()
                if key.lower() not in EXCLUDED_KEYS:
                    result_keys.append(key)
            return result_keys

        # Pattern 1: result = { ... }
        m = re.search(r"\bresult\s*=\s*\{([^}]*)\}", content, re.S)
        if m:
            keys.extend(_extract_keys(m.group(1)))

        # Pattern 2: return { ... } - find all return dicts
        for m in re.finditer(r"\breturn\s*\{([^}]*)\}", content, re.S):
            keys.extend(_extract_keys(m.group(1)))

        # Pattern 3: json.dumps({ ... }) - catches return json.dumps({...})
        for m in re.finditer(r"\bjson\.dumps\s*\(\s*\{([^}]*)\}", content, re.S):
            keys.extend(_extract_keys(m.group(1)))

        # Pattern 4: Parse docstring for 'Output keys added to context'
        m = re.search(r"Output keys added to context[^\n]*\n(.*?)(?:\n\s*\n|\"\"\"|''')", content + '\n\n', re.S | re.I)
        if m:
            doc_block = m.group(1)
            doc_block = re.sub(r'\(.*?\)', '', doc_block)
            for line in doc_block.split('\n'):
                line = line.strip()
                if not line: continue
                line = re.sub(r'[—\-].*', '', line)
                for k in re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', line):
                    if k.lower() not in EXCLUDED_KEYS and k.lower() != 'and':
                        keys.append(k)

    except Exception:
        pass
    return list(dict.fromkeys(keys))  # unique, preserve order


def batch_db_defined_inputs(task_names):
    """
    Get parameter metadata for multiple tasks in a single DB query.
    Returns: { 'task_name': [{'name': 'p1', 'type': 'string', 'description': '...'}, ...], ... }
    """
    if not task_names:
        return {}
    
    unique_names = list(set(filter(None, task_names)))
    if not unique_names:
        return {}

    try:
        rows = DefAsyncTaskParam.query.filter(DefAsyncTaskParam.task_name.in_(unique_names)).all()
        result = {}
        for row in rows:
            if row.task_name not in result:
                result[row.task_name] = []
            result[row.task_name].append({
                "name": row.parameter_name,
                "type": row.data_type or "string",
                "description": row.description or ""
            })
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



