import time
import threading
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

from .manager import ConnectorManager, BaseConnector

class ConnectionRegistry:
    def __init__(self, global_max=200):
        self._engines = {}
        self._lock = threading.Lock()
        self._global_max = global_max

    def get_engine(self, config: dict, uri: str):
        # We'll use a combination of host, port, db, and user as the key 
        # since we don't always have a saved def_connection_id when testing.
        conn_id = config.get('def_connection_id', 'unsaved')
        host = config.get('host', '')
        db_name = config.get('database_name', '')
        user = config.get('username', '')
        
        key = f"{conn_id}:{host}:{db_name}:{user}"
        
        with self._lock:
            if key in self._engines:
                self._engines[key]["last_used"] = time.time()
                return self._engines[key]["engine"]
            
            # Using conservative pool sizes since multiple connections might exist
            engine = create_engine(
                uri,
                pool_size=5, 
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=1800,
            )
            self._engines[key] = {"engine": engine, "last_used": time.time()}
            return engine

# Singleton registry
connection_registry = ConnectionRegistry()

@ConnectorManager.register("postgresql")
@ConnectorManager.register("mysql")
class SQLAlchemyConnector(BaseConnector):
    
    def _build_uri(self) -> str:
        conn_type = self.config.get('connection_type', 'postgresql').lower()
        host = self.config.get('host', 'localhost')
        port = self.config.get('port')
        database = self.config.get('database_name', '')
        username = quote_plus(self.config.get('username', ''))
        
        # TODO: Implement credential vault decryption here once ready
        # e.g., plaintext_password = credential_vault.decrypt(self.config.get('password', ''))
        plaintext_password = self.config.get('password', '')
        password = quote_plus(plaintext_password) if plaintext_password else ''
        
        additional = self.config.get('additional_params', {})

        dialect_map = {
            'postgresql': 'postgresql+psycopg2',
            'mysql': 'mysql+pymysql'
        }
        
        dialect = dialect_map.get(conn_type, 'postgresql+psycopg2')
        
        # Default ports if not specified
        if not port:
            if conn_type == 'postgresql':
                port = 5432
            elif conn_type == 'mysql':
                port = 3306
            else:
                port = 5432

        uri = f"{dialect}://{username}:{password}@{host}:{port}/{database}"
        
        # Add SSL mode if specified
        sslmode = additional.get('sslmode')
        if sslmode:
            uri += f"?sslmode={sslmode}"
            
        return uri

    @property
    def engine(self):
        uri = self._build_uri()
        return connection_registry.get_engine(self.config, uri)

    def test_connection(self) -> tuple:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)

    def fetch_access_points(self) -> list[dict]:
        """Return normalized access point records for sync."""
        # Placeholder for dialect-specific query implementation
        return []

    def fetch_entitlements(self, access_point: dict) -> list[dict]:
        """Return entitlements for a specific access point."""
        # Placeholder for future implementation
        return []

    def get_datasource_metadata(self) -> dict:
        from sqlalchemy import inspect
        inspector = inspect(self.engine)
        
        schemas = inspector.get_schema_names()
        system_schemas = {'information_schema', 'pg_catalog', 'pg_toast', 'mysql', 'performance_schema', 'sys'}
        
        filtered_schemas = [
            s for s in schemas
            if s not in system_schemas and not s.startswith('pg_toast_')
        ]
        
        all_data = []
        total_count = 0
        
        for schema in filtered_schemas:
            tables = inspector.get_table_names(schema=schema)
            views = inspector.get_view_names(schema=schema)
            objects = sorted(tables + views)
            if objects:
                all_data.append({
                    "schema": schema,
                    "tables": objects
                })
                total_count += len(objects)
                
        return {
            "result": all_data,
            "total_tables": total_count
        }

    def get_table_columns(self, table_name: str, schema: str = None) -> list[dict]:
        from sqlalchemy import inspect
        inspector = inspect(self.engine)
        
        if not schema:
            if self.engine.dialect.name == 'postgresql':
                schema = 'public'
            elif self.engine.dialect.name == 'mysql':
                schema = self.engine.url.database
                
        if not inspector.has_table(table_name, schema=schema):
            views = inspector.get_view_names(schema=schema)
            if table_name not in views:
                raise ValueError(f"Table or View '{table_name}' not found in schema '{schema}'")
                
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
            
        return column_details

    def get_table_data(self, table_name: str, schema: str = None, limit: int = 10, offset: int = 0) -> dict:
        from sqlalchemy import inspect, text
        import datetime
        from decimal import Decimal
        
        inspector = inspect(self.engine)
        if not schema:
            if self.engine.dialect.name == 'postgresql':
                schema = 'public'
            elif self.engine.dialect.name == 'mysql':
                schema = self.engine.url.database
                
        if not inspector.has_table(table_name, schema=schema):
            views = inspector.get_view_names(schema=schema)
            if table_name not in views:
                raise ValueError(f"Table or View '{table_name}' not found in schema '{schema}'")

        all_columns = inspector.get_columns(table_name, schema=schema)
        column_names = [c['name'] for c in all_columns]
        
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        primary_keys = pk_constraint.get('constrained_columns', [])

        ordered_columns = []
        for pk in primary_keys:
            if pk in column_names:
                ordered_columns.append(pk)
        for col in column_names:
            if col not in ordered_columns:
                ordered_columns.append(col)
                
        if self.engine.dialect.name == 'mysql':
            quoted_cols = [f'`{c}`' for c in ordered_columns]
            columns_str = ", ".join(quoted_cols)
            count_query = text(f'SELECT COUNT(*) FROM `{schema}`.`{table_name}`')
            data_query = text(f'SELECT {columns_str} FROM `{schema}`.`{table_name}` LIMIT :limit OFFSET :offset')
        else:
            quoted_cols = [f'"{c}"' for c in ordered_columns]
            columns_str = ", ".join(quoted_cols)
            count_query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            data_query = text(f'SELECT {columns_str} FROM "{schema}"."{table_name}" LIMIT :limit OFFSET :offset')

        def _serialize(obj):
            if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, bytes):
                return obj.hex()
            elif isinstance(obj, memoryview):
                return obj.tobytes().hex()
            return obj

        with self.engine.connect() as connection:
            total_count = connection.execute(count_query).scalar()
            result = connection.execute(data_query, {"limit": limit, "offset": offset})
            
            data = []
            for row in result:
                row_dict = dict(row._mapping)
                data.append({k: _serialize(v) for k, v in row_dict.items()})

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            "page": (offset // limit) + 1 if limit > 0 else 1,
            "pages": total_pages,
            "total": total_count,
            "result": data
        }
