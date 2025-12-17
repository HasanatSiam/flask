from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required
from sqlalchemy import text, inspect, and_, case
from executors.extensions import db
from executors.models import InfoSchemaTable, InfoSchemaColumn

from . import data_modeling_bp

@data_modeling_bp.route('/tables', methods=['GET'])
@jwt_required()
def get_all_tables():
    try:
        engine = db.get_engine(bind='db_test')
        inspector = inspect(engine)
        
        schemas = inspector.get_schema_names()
        # Filter out common system schemas to keep the list relevant
        system_schemas = {'information_schema', 'pg_catalog', 'pg_toast'}
        user_schemas = [s for s in schemas if s not in system_schemas and not s.startswith('pg_toast_')]

        all_data = []
        total_count = 0

        for schema in user_schemas:
            tables = inspector.get_table_names(schema=schema)
            views = inspector.get_view_names(schema=schema)
            objects = sorted(tables + views)
            
            if objects:
                all_data.append({
                    "schema": schema,
                    "tables": objects
                })
                total_count += len(objects)

        return make_response(jsonify({
            "schemas": all_data,
            "total_tables": total_count
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch tables",
            "error": str(e)
        }), 500)


@data_modeling_bp.route('/tables/<string:table_name>', methods=['GET'])
@jwt_required()
def get_table_metadata(table_name):
    try:
        schema_name = request.args.get('schema', 'public')

        engine = db.get_engine(bind='db_test')
        inspector = inspect(engine)
        
        if not inspector.has_table(table_name, schema=schema_name):
             # Check if it is a view
             view_names = inspector.get_view_names(schema=schema_name)
             if table_name not in view_names:
                return make_response(jsonify({"message": f"Table or View '{table_name}' not found in schema '{schema_name}'"}), 404)

        columns = inspector.get_columns(table_name, schema=schema_name)
        
        column_details = []
        for col in columns:
            column_details.append({
                "name": col['name'],
                "type": str(col['type']),
                "nullable": col.get('nullable'),
                "default": str(col.get('default')) if col.get('default') else None,
                "primary_key": col.get('primary_key', False)
            })

        return make_response(jsonify({
            "table": table_name,
            "schema": schema_name,
            "columns": column_details
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch table metadata",
            "error": str(e)
        }), 500)


@data_modeling_bp.route('/tables/v1', methods=['GET'])
@jwt_required()
def get_all_tables_v1():
    try:
        # Query full model objects to use .json()
        results = db.session.query(InfoSchemaTable)\
            .filter(InfoSchemaTable.table_schema.notin_(['information_schema', 'pg_catalog', 'pg_toast']))\
            .filter(~InfoSchemaTable.table_schema.like('pg_toast_%'))\
            .order_by(InfoSchemaTable.table_schema, InfoSchemaTable.table_name)\
            .all()

        schema_map = {}
        for row in results:
            data = row.json()
            schema = data['table_schema']
            
            if schema not in schema_map:
                schema_map[schema] = []
            
            schema_map[schema].append(data)

        response_data = []
        for schema in sorted(schema_map.keys()):
            response_data.append({
                "schema": schema,
                "tables": schema_map[schema]
            })

        return make_response(jsonify(response_data), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch tables",
            "error": str(e)
        }), 500)


@data_modeling_bp.route('/tables/v1/<string:table_name>', methods=['GET'])
@jwt_required()
def get_table_metadata_v1(table_name):
    try:
        schema_name = request.args.get('schema', 'public')
        
        # Query only columns from the table
        columns = db.session.query(InfoSchemaColumn).filter(
            InfoSchemaColumn.table_name == table_name,
            InfoSchemaColumn.table_schema == schema_name
        ).all()
        
        if not columns:
             return make_response(jsonify({"message": f"Table or View '{table_name}' not found in schema '{schema_name}'"}), 404)

        column_details = [col.json() for col in columns]

        return make_response(jsonify(column_details), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch table metadata",
            "error": str(e)
        }), 500)

