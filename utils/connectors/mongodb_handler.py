"""
MongoDB Connector
=================
Implements BaseConnector for MongoDB using pymongo.

Supported auth modes
--------------------
1. URI-based  : provide ``uri`` (e.g. ``mongodb+srv://user:pass@cluster.mongodb.net``)
2. Host-based : provide ``host``, ``port`` (default 27017), ``username``,
                ``password``, and optionally ``auth_source`` (default "admin").

Config keys (all optional when ``uri`` is supplied)
----------------------------------------------------
uri           - Full MongoDB connection string. Overrides all other fields.
host          - Hostname or IP of the MongoDB server.
port          - Port number (default 27017).
username      - Auth username.
password      - Auth password.
auth_source   - Authentication database (default "admin").
database      - Default database to browse. If omitted, all user databases
                are listed in get_datasource_metadata().
tls           - "true"/"false" or bool. Enable TLS (default False).
additional_params - dict of extra key/value pairs forwarded to MongoClient.
"""

import threading
import time
from urllib.parse import quote_plus

from .manager import ConnectorManager, BaseConnector


# ---------------------------------------------------------------------------
# Connection registry - reuse MongoClient instances across requests.
# MongoClient is already thread-safe and manages its own connection pool,
# so one client per unique connection config is sufficient.
# ---------------------------------------------------------------------------

class _MongoClientRegistry:
    def __init__(self):
        self._clients: dict = {}
        self._lock = threading.Lock()

    def _make_key(self, config: dict) -> str:
        conn_id = config.get("def_connection_id", "unsaved")
        host    = config.get("host", "")
        port    = config.get("port", 27017)
        db      = config.get("database", "")
        user    = config.get("username", "")
        return f"{conn_id}:{host}:{port}:{db}:{user}"

    def get_client(self, config: dict):
        """Return a cached MongoClient, creating one if needed."""
        from pymongo import MongoClient

        key = self._make_key(config)
        with self._lock:
            if key in self._clients:
                self._clients[key]["last_used"] = time.time()
                return self._clients[key]["client"]

            client = MongoClient(_build_mongo_uri(config), serverSelectionTimeoutMS=5000)
            self._clients[key] = {"client": client, "last_used": time.time()}
            return client


_registry = _MongoClientRegistry()


# Helper: build a MongoDB URI from config dict


def _build_mongo_uri(config: dict) -> str:
    """
    Construct a MongoDB connection URI from the config dictionary.
    If ``uri`` is already set in config (or inside additional_params),
    it is returned as-is — this covers saved connections that store the
    Atlas SRV string inside additional_params.
    """
    # Top-level uri takes priority
    if config.get("uri"):
        return config["uri"]

    # URI may have been saved inside additional_params (e.g. via the save-connection API)
    additional = config.get("additional_params") or {}
    if additional.get("uri"):
        return additional["uri"]

    host     = config.get("host", "localhost")
    port     = config.get("port", 27017)
    username = config.get("username", "")
    password = config.get("password", "")
    auth_src = config.get("auth_source", "admin")

    # Support additional_params for things like tls, replicaSet, etc.
    tls = config.get("tls") or additional.get("tls", False)
    if isinstance(tls, str):
        tls = tls.lower() == "true"

    if username and password:
        user_info = f"{quote_plus(username)}:{quote_plus(password)}@"
    elif username:
        user_info = f"{quote_plus(username)}@"
    else:
        user_info = ""

    uri = f"mongodb://{user_info}{host}:{port}/"

    params = []
    if username:
        params.append(f"authSource={auth_src}")
    if tls:
        params.append("tls=true")

    # Forward any extra string params from additional_params
    skip = {"tls", "auth_source"}
    for k, v in additional.items():
        if k not in skip and v is not None:
            params.append(f"{k}={v}")

    if params:
        uri += "?" + "&".join(params)

    return uri


# Type mapper: map Python / BSON types to friendly type strings


_BSON_TYPE_MAP = {
    "str":        "string",
    "int":        "integer",
    "float":      "float",
    "bool":       "boolean",
    "datetime":   "datetime",
    "Decimal128": "decimal",
    "ObjectId":   "string",   # treat ObjectId as string for consumers
    "list":       "array",
    "dict":       "object",
    "NoneType":   "null",
    "bytes":      "binary",
}


def _infer_type(value) -> str:
    """Map a Python value to a friendly type string."""
    type_name = type(value).__name__
    return _BSON_TYPE_MAP.get(type_name, type_name)



# System databases to hide from the user


_SYSTEM_DBS = {"admin", "local", "config"}



# Connector


@ConnectorManager.register("mongodb")
class MongoDBConnector(BaseConnector):
    """
    MongoDB connector built on pymongo.

    Collections are treated as "tables" and field names are inferred from
    a sample of documents (up to _SAMPLE_SIZE docs) since MongoDB is
    schema-less.
    """

    # Number of documents sampled to infer field names / types
    _SAMPLE_SIZE: int = 100

    def __init__(self, config: dict):
        super().__init__(config)
        # Per-instance field-inference cache: {(database, collection): [columns]}
        self._columns_cache: dict = {}


    # Internal helpers


    @property
    def _client(self):
        """Lazy-cached MongoClient from the shared registry."""
        return _registry.get_client(self.config)

    def _list_user_databases(self) -> list:
        """Return all non-system database names visible to this client."""
        try:
            db_names = self._client.list_database_names()
        except Exception as exc:
            raise RuntimeError(f"Failed to list databases: {exc}") from exc
        return sorted(n for n in db_names if n not in _SYSTEM_DBS)

    def _infer_columns(self, db_name: str, collection_name: str) -> list:
        """
        Sample up to _SAMPLE_SIZE documents from a collection and return
        a unified list of field definitions with inferred types.

        Results are cached per (db_name, collection_name) pair for the
        lifetime of this connector instance.
        """
        cache_key = (db_name, collection_name)
        if cache_key in self._columns_cache:
            return self._columns_cache[cache_key]

        db  = self._client[db_name]
        col = db[collection_name]

        # Aggregate field -> type mapping across sampled docs
        field_types: dict = {}
        for doc in col.find({}, limit=self._SAMPLE_SIZE):
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = _infer_type(value)

        if not field_types:
            # Empty collection - return at least _id
            field_types = {"_id": "string"}

        columns = [
            {
                "name":        field,
                "type":        ftype,
                "nullable":    True,   # MongoDB docs are always nullable
                "default":     None,
                "primary_key": field == "_id",
            }
            for field, ftype in field_types.items()
        ]

        self._columns_cache[cache_key] = columns
        return columns

    @staticmethod
    def _serialize_doc(doc: dict) -> dict:
        """
        Recursively convert non-JSON-serializable BSON types to strings
        so the result can be safely returned as JSON.
        """
        from bson import ObjectId
        from bson.decimal128 import Decimal128
        import datetime

        result = {}
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                result[k] = str(v)
            elif isinstance(v, Decimal128):
                result[k] = float(v.to_decimal())
            elif isinstance(v, (datetime.datetime, datetime.date)):
                result[k] = v.isoformat()
            elif isinstance(v, bytes):
                result[k] = v.hex()
            elif isinstance(v, dict):
                result[k] = MongoDBConnector._serialize_doc(v)
            elif isinstance(v, list):
                result[k] = [
                    MongoDBConnector._serialize_doc(i) if isinstance(i, dict) else str(i)
                    for i in v
                ]
            else:
                result[k] = v
        return result

    def test_connection(self) -> tuple:
        """
        Ping the MongoDB server to verify connectivity and credentials.
        Returns (True, "Connection successful") or (False, "<reason>").
        """
        try:
            self._client.admin.command("ping")
            return True, "Connection successful"
        except Exception:
            # Sanitized message - avoid leaking host/credential details
            return False, "Connection failed. Check your host, port, and credentials."

    def fetch_access_points(self) -> list:
        """
        Return collections as access points for access-control sync.
        Each access point represents one collection in the target database.
        """
        try:
            db_name   = self.config.get("database")
            databases = [db_name] if db_name else self._list_user_databases()

            access_points = []
            for db in databases:
                for collection in self._client[db].list_collection_names():
                    access_points.append({
                        "name":     f"{db}.{collection}",
                        "type":     "collection",
                        "database": db,
                    })
            return access_points
        except Exception:
            return []

    def fetch_entitlements(self, access_point: dict) -> list:
        """
        Placeholder - MongoDB roles/entitlements are managed at the server
        level via the $listRoles command and are highly environment-specific.
        Implement here when access-control sync is needed.
        """
        return []

    def get_datasource_metadata(self) -> dict:
        """
        Returns all user databases and their collections, structured as:

            {
                "result": [
                    {"schema": "<db_name>", "tables": ["col1", "col2", ...]},
                    ...
                ],
                "total_tables": <int>
            }

        If ``database`` is set in config, only that database is listed.
        """
        # Support both 'database' (direct config) and 'database_name' (saved DB model field)
        db_name   = self.config.get("database") or self.config.get("database_name")
        databases = [db_name] if db_name else self._list_user_databases()

        result     = []
        total_cols = 0

        for db in databases:
            try:
                collections = sorted(self._client[db].list_collection_names())
            except Exception:
                collections = []

            if collections:
                result.append({"schema": db, "tables": collections})
                total_cols += len(collections)

        return {
            "result":       result,
            "total_tables": total_cols,
        }

    def get_table_columns(self, table_name: str, schema: str = None) -> list:
        """
        Infer field definitions for a MongoDB collection by sampling documents.

        Parameters
        ----------
        table_name : str
            Collection name.
        schema : str, optional
            Database name. Falls back to config["database"].

        Returns
        -------
        list[dict]
            Each dict has keys: name, type, nullable, default, primary_key.
        """
        db_name = schema or self.config.get("database") or self.config.get("database_name")
        if not db_name:
            raise ValueError(
                "No database specified. Provide 'database' in config or pass schema= argument."
            )

        # Validate collection exists
        db = self._client[db_name]
        if table_name not in db.list_collection_names():
            raise ValueError(
                f"Collection '{table_name}' not found in database '{db_name}'."
            )

        return self._infer_columns(db_name, table_name)

    def get_table_data(
        self,
        table_name: str,
        schema: str = None,
        limit: int = 10,
        offset: int = 0,
    ) -> dict:
        """
        Return paginated documents from a MongoDB collection.

        Parameters
        ----------
        table_name : str
            Collection name.
        schema : str, optional
            Database name. Falls back to config["database"].
        limit : int
            Number of documents to return (default 10).
        offset : int
            Number of documents to skip for pagination (default 0).

        Returns
        -------
        dict
            {
                "page":   <current_page_number>,
                "pages":  <total_pages>,
                "total":  <total_document_count>,
                "result": [<serialized_doc>, ...]
            }
        """
        db_name = schema or self.config.get("database") or self.config.get("database_name")
        if not db_name:
            raise ValueError(
                "No database specified. Provide 'database' in config or pass schema= argument."
            )

        db  = self._client[db_name]
        col = db[table_name]

        # Validate collection exists
        if table_name not in db.list_collection_names():
            raise ValueError(
                f"Collection '{table_name}' not found in database '{db_name}'."
            )

        total_count = col.count_documents({})
        cursor      = col.find({}).skip(offset).limit(limit)

        data = [self._serialize_doc(doc) for doc in cursor]

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            "page":   (offset // limit) + 1 if limit > 0 else 1,
            "pages":  total_pages,
            "total":  total_count,
            "result": data,
        }
