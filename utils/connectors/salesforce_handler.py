from simple_salesforce import Salesforce
from .manager import ConnectorManager, BaseConnector

@ConnectorManager.register("salesforce")
class SalesforceConnector(BaseConnector):

    # Map Salesforce SOAP/REST types to friendly type strings.
    # "reference" is an intentional shared vocabulary term used across connectors
    # (e.g. ServiceNow also maps its reference/GlideRecord type to "reference")
    # to represent FK-style pointers to other records in a connector-agnostic way.
    _SF_TYPE_MAP = {
        "string": "string",
        "textarea": "string",
        "int": "integer",
        "boolean": "boolean",
        "datetime": "datetime",
        "date": "date",
        "time": "time",
        "double": "float",
        "currency": "decimal",
        "url": "string",
        "email": "string",
        "phone": "string",
        "id": "string",
        "reference": "reference",  # lookup / master-detail relationship field
    }

    # ------------------------------------------------------------------
    # Internal: client construction
    # ------------------------------------------------------------------

    def _build_client(self) -> Salesforce:
        """
        Instantiate Salesforce client.
        Supports both:
          1. Basic Auth: username, password, security_token
          2. OAuth/Session ID Auth: session_id, instance_url (often used for ECAs)
        """
        session_id = self.config.get('session_id')
        instance_url = self.config.get('instance_url')

        if session_id and instance_url:
            # Clean URL format
            instance_url = instance_url.replace('https://', '').replace('http://', '').split('/')[0]
            return Salesforce(
                instance=instance_url,
                session_id=session_id
            )

        # Fallback to username/password (Connected Apps)
        username = self.config.get('username', '')
        password = self.config.get('password', '')

        # Extract from root or additional_params nested dictionary
        additional = self.config.get('additional_params') or {}
        security_token = self.config.get('security_token') or additional.get('security_token', '')
        domain = self.config.get('domain') or additional.get('domain') or 'login'

        # Check if we should authenticate via Connected App Client ID + Client Secret (OAuth)
        client_id = self.config.get('client_id') or additional.get('client_id')
        client_secret = self.config.get('client_secret') or additional.get('client_secret')

        # Get custom host if provided (e.g. mydomain.develop.my.salesforce.com)
        custom_host = self.config.get('host') or additional.get('host')
        if custom_host:
            # Clean protocol and path
            auth_host = custom_host.replace('https://', '').replace('http://', '').split('/')[0]
        else:
            auth_host = f"{domain}.salesforce.com"

        # If OAuth Client Credentials (client_id/client_secret) are provided, do REST OAuth 2.0 flow.
        if client_id and client_secret:
            import requests
            token_url = f"https://{auth_host}/services/oauth2/token"
            payload = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            resp = requests.post(token_url, data=payload)
            res_data = resp.json()
            
            if 'access_token' in res_data:
                access_token = res_data.get("access_token")
                # Ensure the instance URL is properly formatted for simple_salesforce
                instance_url = res_data.get("instance_url", "").replace('https://', '').replace('http://', '').split('/')[0]
                return Salesforce(
                    instance=instance_url,
                    session_id=access_token
                )
            else:
                raise ValueError(f"OAuth Client Credentials failed: {res_data}")

        # If no client credentials, use simple-salesforce's default authentication (uses SOAP login)
        return Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )

    # ------------------------------------------------------------------
    # Fix 1: Lazy-cached client property — builds once per connector
    # instance, mirroring the ConnectionRegistry pattern in sql_handler.
    # ------------------------------------------------------------------

    @property
    def _client(self) -> Salesforce:
        """
        Returns a cached Salesforce client, building it on first access.
        Avoids repeated re-authentication across get_table_columns() and
        get_table_data() calls within the same request.
        """
        if not hasattr(self, '_sf_client') or self._sf_client is None:
            self._sf_client: Salesforce = self._build_client()
        return self._sf_client

    # Fix 1 (cont.): Per-instance describe cache — avoids re-fetching the
    # same SObject field list when get_table_data() calls get_table_columns().
    _describe_cache: dict = {}

    # ------------------------------------------------------------------
    # Fix 2: Validate table_name against the known-queryable SObject list
    # ------------------------------------------------------------------

    def _get_queryable_objects(self) -> set:
        """
        Returns the set of queryable SObject names for this org, cached for
        the life of this connector instance. Used to validate table_name
        before interpolating it into SOQL, preventing injection.
        """
        if not hasattr(self, '_queryable_objects_cache'):
            meta = self._client.describe()
            self._queryable_objects_cache: set = {
                obj['name']
                for obj in meta.get('sobjects', [])
                if obj.get('queryable', False)
            }
        return self._queryable_objects_cache

    def _validate_table_name(self, table_name: str) -> None:
        """
        Raises ValueError if table_name is not a known queryable SObject.
        Call this before using table_name in any SOQL string.
        """
        queryable = self._get_queryable_objects()
        if table_name not in queryable:
            raise ValueError(
                f"'{table_name}' is not a queryable Salesforce SObject. "
                f"Use get_datasource_metadata() to retrieve valid object names."
            )

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def test_connection(self) -> tuple:
        """
        Test Salesforce connection by authenticating and performing a metadata call.
        """
        try:
            self._client.describe()
            return True, "Connection successful"
        except ValueError as e:
            # Our own controlled error messages (e.g. OAuth failure) — safe to surface
            return False, str(e)
        except Exception:
            # Fix 4: Avoid leaking raw exception details (auth endpoints, partial
            # config values) to the caller. Return a generic, sanitized message.
            return False, "Connection failed. Check your credentials and endpoint configuration."

    def get_datasource_metadata(self) -> dict:
        """
        Returns all queryable Salesforce Objects.
        Schema is None because Salesforce has no database schema concept.
        """
        queryable = self._get_queryable_objects()
        tables = sorted(queryable)

        return {
            "result": [
                {
                    "schema": None,
                    "tables": tables
                }
            ],
            "total_tables": len(tables)
        }

    def get_table_columns(self, table_name: str, schema: str = None) -> list:
        """
        Returns column (field) definitions for the specified SObject.
        Uses the cached client and caches the describe() result per object.
        """
        # Fix 2: Validate before any API call
        self._validate_table_name(table_name)

        # Fix 1: Use cached describe result if available
        if table_name not in self._describe_cache:
            self._describe_cache[table_name] = getattr(self._client, table_name).describe()

        desc = self._describe_cache[table_name]

        columns = []
        for field in desc.get('fields', []):
            raw_type = field.get('type', 'string')
            friendly_type = self._SF_TYPE_MAP.get(str(raw_type), "string")

            columns.append({
                "name": field['name'],
                "type": friendly_type,
                "nullable": field.get('nillable', True),
                "default": field.get('defaultValue'),
                "primary_key": field['name'].lower() == 'id'
            })

        return columns

    def get_table_data(
        self,
        table_name: str,
        schema: str = None,
        limit: int = 10,
        offset: int = 0,
    ) -> dict:
        """
        Returns paginated records using SOQL (Salesforce Object Query Language).

        Pagination limits
        -----------------
        Salesforce SOQL enforces a hard cap of OFFSET <= 2000. Requesting
        beyond that raises a SalesforceMalformedRequest. This method surfaces
        a clear ValueError at the boundary rather than letting that bubble up.
        For tables with more than ~2000 rows, cursor-based pagination via
        nextRecordsUrl (returned in sf.query_more()) is the recommended
        Salesforce-native approach and would require a stateful pagination API.
        """
        # Fix 3: Guard against the SOQL OFFSET hard cap
        if offset > 2000:
            raise ValueError(
                f"Salesforce SOQL OFFSET is capped at 2000 records. "
                f"Requested offset {offset} exceeds this limit. "
                f"Use cursor-based pagination (nextRecordsUrl) for large datasets."
            )

        sf = self._client  # Fix 1: single shared client

        # 1. Fetch column metadata (uses cache — no second describe() call)
        columns = self.get_table_columns(table_name)
        col_names = [c['name'] for c in columns]
        fields_str = ", ".join(col_names)
        # field names come from the server-side describe() response — safe to interpolate

        # 2. Get total size of the Object dataset
        count_res = sf.query(f"SELECT COUNT() FROM {table_name}")
        total_count = count_res.get('totalSize', 0)

        # 3. Request paginated records using LIMIT and OFFSET
        # table_name is validated against the known SObject list above (Fix 2)
        query_str = f"SELECT {fields_str} FROM {table_name} LIMIT {limit} OFFSET {offset}"
        res = sf.query(query_str)

        # Flatten records: simple-salesforce prefixes each record with an 'attributes' dict
        data = []
        for row in res.get('records', []):
            row.pop('attributes', None)
            data.append(row)

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            "page": (offset // limit) + 1 if limit > 0 else 1,
            "pages": total_pages,
            "total": total_count,
            "result": data
        }
