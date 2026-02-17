from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required
from sqlalchemy import text, inspect, and_, case, create_engine
import datetime
from decimal import Decimal
from urllib.parse import quote_plus
from executors.extensions import db
from executors.models import InfoSchemaTable, InfoSchemaColumn, DefDataSource, DefDataSourceConnection

from . import data_modeling_bp


def _get_engine_for_datasource(datasource_name):
    """
    Helper function to get SQLAlchemy engine for a specific datasource.
    Looks up datasource by name and creates engine from connection details.
    """
    # Find datasource by name
    datasource = DefDataSource.query.filter_by(datasource_name=datasource_name).first()
    if not datasource:
        raise ValueError(f"Datasource '{datasource_name}' not found")
    
    # Get active connection for this datasource
    connection = DefDataSourceConnection.query.filter_by(
        def_data_source_id=datasource.def_data_source_id,
        is_active=True
    ).order_by(DefDataSourceConnection.def_connection_id.desc()).first()
    
    if not connection:
        raise ValueError(f"No active connection found for datasource '{datasource_name}'")
    
    # Build connection URI (currently supports PostgreSQL)
    if connection.connection_type.lower() == 'postgresql':
        host = connection.host or 'localhost'
        port = connection.port or 5432
        database = connection.database_name or ''
        username = quote_plus(connection.username or '')
        password = quote_plus(connection.password or '')
        
        uri = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        
        # Add SSL mode if specified
        if connection.additional_params and 'sslmode' in connection.additional_params:
            uri += f"?sslmode={connection.additional_params['sslmode']}"
        
        return create_engine(uri, pool_pre_ping=True)
    else:
        raise ValueError(f"Unsupported connection type: {connection.connection_type}")


def _serialize_data(obj):
    """
    Helper to make object JSON serializable.
    Handles datetime, Decimal, bytes, memoryview, etc.
    """
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.hex()
    elif isinstance(obj, memoryview):
        return obj.tobytes().hex()
    elif isinstance(obj, dict):
        return {k: _serialize_data(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_serialize_data(item) for item in obj]
    return obj


@data_modeling_bp.route('/tables', methods=['GET'])
@jwt_required()
def tables_handler():
    try:
        table_name = request.args.get('table')
        schema_name = request.args.get('schema')

        try:
            engine = db.get_engine(bind='db_test')
        except KeyError:
            engine = db.engine
        inspector = inspect(engine)

        # -------------------------------
        # CASE 1: TABLE METADATA REQUEST
        # /tables?table=users
        # /tables?table=users&schema=public
        # -------------------------------
        if table_name:
            schema = schema_name or 'public'

            # Check table or view existence
            if not inspector.has_table(table_name, schema=schema):
                views = inspector.get_view_names(schema=schema)
                if table_name not in views:
                    return make_response(jsonify({
                        "message": f"Table or View '{table_name}' not found in schema '{schema}'"
                    }), 404)

            columns = inspector.get_columns(table_name, schema=schema)

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
                "schema": schema,
                "table": table_name,
                "columns": column_details
            }), 200)

        # ---------------------------------
        # CASE 2: LIST TABLES / SCHEMAS
        # /tables
        # /tables?schema=public
        # ---------------------------------
        schemas = inspector.get_schema_names()
        system_schemas = {'information_schema', 'pg_catalog', 'pg_toast'}

        if schema_name:
            schemas = [schema_name]
        else:
            schemas = [
                s for s in schemas
                if s not in system_schemas and not s.startswith('pg_toast_')
            ]

        all_data = []
        total_count = 0

        for schema in schemas:
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
            "message": "Failed to fetch table information",
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


@data_modeling_bp.route('/datasource/metadata', methods=['GET'])
@jwt_required()
def get_datasource_metadata():
    """
    Get schemas and tables for a specific datasource.
    Query params: datasource_name (required)
    """
    try:
        datasource_name = request.args.get('datasource_name')
        if not datasource_name:
            return make_response(jsonify({
                'message': 'datasource_name query parameter is required'
            }), 400)

        # Get engine for the datasource
        engine = _get_engine_for_datasource(datasource_name)
        inspector = inspect(engine)

        # Get all schemas
        schemas = inspector.get_schema_names()
        system_schemas = {'information_schema', 'pg_catalog', 'pg_toast'}

        # Filter out system schemas
        schemas = [
            s for s in schemas
            if s not in system_schemas and not s.startswith('pg_toast_')
        ]

        all_data = []
        total_count = 0

        for schema in schemas:
            tables = inspector.get_table_names(schema=schema)
            views = inspector.get_view_names(schema=schema)
            objects = sorted(tables + views)

            if objects:
                all_data.append({
                    "schema": schema,
                    "tables": objects
                })
                total_count += len(objects)

        # Dispose engine
        engine.dispose()

        return make_response(jsonify({
            "datasource_name": datasource_name,
            "result": all_data,
            "total_tables": total_count
        }), 200)

    except ValueError as ve:
        return make_response(jsonify({
            "message": str(ve)
        }), 404)
    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch datasource metadata",
            "error": str(e)
        }), 500)


@data_modeling_bp.route('/table/columns', methods=['GET'])
@jwt_required()
def get_table_columns():
    """
    Get column metadata for a specific table or all tables.
    Query params: 
        - datasource_name (required)
        - table_name (optional) - If provided, returns detailed columns for that table. If omitted, returns list of tables with column names for the datasource.
        - schema (optional) - Default 'public' if table_name is provided. If table_name is omitted, defaults to all schemas.
    """
    try:
        table_name = request.args.get('table_name')
        datasource_name = request.args.get('datasource_name')
        
        if not datasource_name:
            return make_response(jsonify({
                'message': 'datasource_name query parameter is required'
            }), 400)

        if not table_name:
            schema_name = request.args.get('schema')
            engine = _get_engine_for_datasource(datasource_name)
            try:
                inspector = inspect(engine)

                if schema_name:
                    schemas = [schema_name]
                else:
                    all_schemas = inspector.get_schema_names()
                    system_schemas = {'information_schema', 'pg_catalog', 'pg_toast'}
                    schemas = [
                        s for s in all_schemas
                        if s not in system_schemas and not s.startswith('pg_toast_')
                    ]
                
                result_list = []
                for s in schemas:
                    tables = inspector.get_table_names(schema=s)
                    for t in tables:
                        cols = inspector.get_columns(t, schema=s)
                        col_names = [c['name'] for c in cols]
                        
                        entry = {
                            "table": t,
                            "columns": col_names
                        }
                        if not schema_name:
                            entry["schema"] = s
                        
                        result_list.append(entry)

                response = {
                    "datasource_name": datasource_name,
                    "result": result_list
                }
                if schema_name:
                    response["schema"] = schema_name
                
                engine.dispose()
                return make_response(jsonify(response), 200)

            except Exception as e:
                engine.dispose()
                raise e

        schema_name = request.args.get('schema', 'public')

        # Get engine for the datasource
        engine = _get_engine_for_datasource(datasource_name)
        inspector = inspect(engine)

        # Check table or view existence
        if not inspector.has_table(table_name, schema=schema_name):
            views = inspector.get_view_names(schema=schema_name)
            if table_name not in views:
                engine.dispose()
                return make_response(jsonify({
                    "message": f"Table or View '{table_name}' not found in schema '{schema_name}'"
                }), 404)

        # Get columns
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

        # Dispose engine
        engine.dispose()

        return make_response(jsonify({
            "datasource_name": datasource_name,
            "schema": schema_name,
            "table": table_name,
            "result": column_details
        }), 200)

    except ValueError as ve:
        return make_response(jsonify({
            "message": str(ve)
        }), 404)
    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch table columns",
            "error": str(e)
        }), 500)


@data_modeling_bp.route('/table/data', methods=['GET'])
@jwt_required()
def get_table_data():
    """
    Get all data for a specific table with pagination.
    Query params: 
        - table_name (required)
        - datasource_name (required)
        - schema (optional, default='public')
        - page (optional, default=1)
        - per_page (optional, default=10)
    """
    try:
        table_name = request.args.get('table_name')
        datasource_name = request.args.get('datasource_name')
        
        if not table_name:
            return make_response(jsonify({
                'message': 'table_name query parameter is required'
            }), 400)
        
        if not datasource_name:
            return make_response(jsonify({
                'message': 'datasource_name query parameter is required'
            }), 400)

        schema_name = request.args.get('schema', 'public')
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        offset = (page - 1) * per_page

        # Get engine for the datasource
        engine = _get_engine_for_datasource(datasource_name)
        inspector = inspect(engine)

        # Check table or view existence
        if not inspector.has_table(table_name, schema=schema_name):
            views = inspector.get_view_names(schema=schema_name)
            if table_name not in views:
                engine.dispose()
                return make_response(jsonify({
                    "message": f"Table or View '{table_name}' not found in schema '{schema_name}'"
                }), 404)

        # Get all columns and primary keys
        all_columns = inspector.get_columns(table_name, schema=schema_name)
        column_names = [c['name'] for c in all_columns]
        
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema_name)
        primary_keys = pk_constraint.get('constrained_columns', [])

        # Order columns: Primary Keys first, then the rest
        ordered_columns = []
        for pk in primary_keys:
            if pk in column_names:
                ordered_columns.append(pk)
        
        for col in column_names:
            if col not in ordered_columns:
                ordered_columns.append(col)

        # Build paginated query with explicit column order
        quoted_cols = [f'"{c}"' for c in ordered_columns]
        columns_str = ", ".join(quoted_cols)
        
        count_query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
        data_query = text(f'SELECT {columns_str} FROM "{schema_name}"."{table_name}" LIMIT :limit OFFSET :offset')

        with engine.connect() as connection:
            # Get total count
            total_count = connection.execute(count_query).scalar()
            
            # Get paginated data
            result = connection.execute(data_query, {"limit": per_page, "offset": offset})
            
            # Convert result rows to list of dicts and handle serialization
            data = []
            for row in result:
                # RowMapping handles order as per the SELECT statement
                row_dict = dict(row._mapping)
                data.append(_serialize_data(row_dict))

        # Dispose engine
        engine.dispose()

        # Calculate total pages
        total_pages = (total_count + per_page - 1) // per_page

        return make_response(jsonify({
            "datasource_name": datasource_name,
            "schema": schema_name,
            "table": table_name,
            "page": page,
            "pages": total_pages,
            "total": total_count,
            "result": data
        }), 200)

    except ValueError as ve:
        return make_response(jsonify({
            "message": str(ve)
        }), 404)
    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to fetch table data",
            "error": str(e)
        }), 500)



