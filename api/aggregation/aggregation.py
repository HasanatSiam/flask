from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required
from sqlalchemy import text
from executors.extensions import db
import re

from . import aggregation_bp    




@aggregation_bp.route('/create_aggregate_table', methods=['POST'])
@jwt_required()
def create_aggregate_table():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "No JSON data provided"}), 400)

        materialized_view_name = data.get("materialized_view_name")
        schema_name = data.get("schema_name", "public")

        if not materialized_view_name:
            return make_response(jsonify({"message": "Materialized view name is required"}), 400)
        
        engine = db.get_engine(bind='db_test')

        # Construct SQL with both arguments
        sql = text(f"SELECT create_imat('{materialized_view_name}', '{schema_name}')")

        with engine.connect() as connection:
            connection.execute(sql)
            connection.commit()

        return make_response(jsonify({
            "message": f"The aggregate table '{materialized_view_name}' has been successfully provisioned within schema '{schema_name}'.",
            "details": {
                "view_name": materialized_view_name,
                "schema": schema_name
            }
        }), 201)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to create aggregate table",
            "error": str(e)
        }), 500)







@aggregation_bp.route("/create_mv", methods=["POST"])
def create_mv_endpoint():
    """
    API to create a materialized view with structured payload.
    All helper functions are inside the route.
    """    
    IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    def validate_ident(name):
        if not isinstance(name, str):
            raise ValueError(f"Identifier must be a string, got: {name!r}")
        if not IDENT_RE.match(name):
            raise ValueError(f"Invalid identifier: {name!r}")

    def build_column_expr(col_def):
        """
        Build SQL for one select item.
        Supports:
        - simple column
        - aggregate functions (COUNT, SUM, AVG, etc.)
        - functions (date_trunc, custom functions)
        - aliasing
        """
        if not isinstance(col_def, dict):
            raise ValueError(f"Select item must be a dict, got: {col_def!r}")

        expr = None

        # 1️⃣ Simple column
        if 'column' in col_def and 'aggregate' not in col_def and 'function' not in col_def:
            expr = col_def['column']

        # 2️⃣ Aggregate function
        elif 'aggregate' in col_def:
            col = col_def.get('column')
            if col is None:
                raise ValueError("Aggregate item must have a 'column' key")
            func = col_def['aggregate'].upper()
            distinct = col_def.get('distinct', False)

            if col == "*":
                expr = f"{func}(*)"
            else:
                expr = f"{func}({('DISTINCT ' if distinct else '')}{col})"

        # 3️⃣ Function call
        elif 'function' in col_def:
            func = col_def['function']
            func_name = func.get('name')
            if not func_name:
                raise ValueError("Function must have a 'name'")
            args = func.get('args', [])
            if not isinstance(args, list):
                raise ValueError("Function 'args' must be a list")
            args_list = []
            for arg in args:
                if isinstance(arg, dict) and 'column' in arg:
                    args_list.append(arg['column'])
                else:
                    args_list.append(str(arg))
            expr = f"{func_name}({', '.join(args_list)})"

        else:
            raise ValueError(f"Unknown select item: {col_def}")

        # 4️⃣ Alias
        alias = col_def.get('alias')
        if alias:
            validate_ident(alias)
            expr += f" AS {alias}"

        if expr is None:
            raise ValueError(f"Failed to build SQL expression from: {col_def}")

        return expr


    def build_select_clause(select_list):
        if not select_list:
            raise ValueError("select list cannot be empty")
        return ", ".join(build_column_expr(item) for item in select_list)

    def build_from_clause(from_obj):
        schema = from_obj.get("schema", "public")
        table = from_obj["table"]
        alias = from_obj.get("alias")
        validate_ident(schema)
        validate_ident(table)
        if alias:
            validate_ident(alias)
        base = f"{schema}.{table}"
        return f"{base} {alias}" if alias else base

    def build_join_clause(joins):
        if not joins:
            return ""
        parts = []
        for j in joins:
            jtype = j.get("type", "INNER").upper()
            schema = j["schema"]
            table = j["table"]
            alias = j.get("alias")
            validate_ident(schema)
            validate_ident(table)
            if alias:
                validate_ident(alias)
            tbl = f"{schema}.{table}" + (f" {alias}" if alias else "")
            conditions = j.get("conditions", [])
            if not conditions:
                raise ValueError(f"Join {tbl} must have at least one condition")
            cond_exprs = []
            for cond in conditions:
                cond_exprs.append(f"{cond['left']} {cond['op']} {cond['right']}")
            cond_str = " AND ".join(cond_exprs)
            parts.append(f"{jtype} JOIN {tbl} ON {cond_str}")
        return " ".join(parts)

    def build_group_by_clause(group_by_list):
        if not group_by_list:
            return ""
        exprs = []
        for item in group_by_list:
            if 'column' in item:
                exprs.append(item['column'])
            elif 'function' in item:
                func = item['function']
                args = func.get('args', [])
                if not args:
                    raise ValueError("Function in group_by must have args")
                args_list = []
                for arg in args:
                    if isinstance(arg, dict) and 'column' in arg:
                        args_list.append(arg['column'])
                    else:
                        args_list.append(str(arg))
                exprs.append(f"{func['name']}({', '.join(args_list)})")
            else:
                raise ValueError(f"Unknown group_by item: {item}")
        return ", ".join(exprs)

    # -------------------------
    # PARSE PAYLOAD
    # -------------------------
    payload = request.get_json()
    if not payload:
        return jsonify({"ok": False, "error": "JSON payload required"}), 400

    mv_name = payload.get("mv_name")
    mv_schema = payload.get("mv_schema", "imat")
    select_list = payload.get("select")
    from_obj = payload.get("from")
    joins = payload.get("joins", [])
    group_by = payload.get("group_by", [])

    # validate identifiers
    try:
        validate_ident(mv_name)
        validate_ident(mv_schema)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if not select_list or not from_obj:
        return jsonify({"ok": False, "error": "select[] and from{} are required"}), 400

    # -------------------------
    # BUILD SQL
    # -------------------------
    try:
        select_clause = build_select_clause(select_list)
        from_clause = build_from_clause(from_obj)
        join_clause = build_join_clause(joins)
        group_clause = build_group_by_clause(group_by)

        sql = f"SELECT {select_clause} FROM {from_clause}"
        if join_clause:
            sql += " " + join_clause
        if group_clause:
            sql += f" GROUP BY {group_clause}"

    except Exception as e:
        return jsonify({"ok": False, "error": f"Error building SQL: {str(e)}"}), 400

    # -------------------------
    # CREATE MATERIALIZED VIEW
    # -------------------------
    create_mv_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {mv_schema};
    DROP MATERIALIZED VIEW IF EXISTS {mv_schema}.{mv_name} CASCADE;
    CREATE MATERIALIZED VIEW {mv_schema}.{mv_name} AS {sql} WITH NO DATA;
    REFRESH MATERIALIZED VIEW {mv_schema}.{mv_name};
    """

    engine = db.get_engine(bind='db_test')
    try:
        conn = engine.connect()
        conn.execute(text(create_mv_sql))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({
            "error": "Failed creating MV",
            "detail": str(e),
            "sql": sql
        }), 500

    return jsonify({
        "message": "Materialized view created successfully",
        "mv": f"{mv_schema}.{mv_name}",
        "generated_sql": sql
    }), 201

