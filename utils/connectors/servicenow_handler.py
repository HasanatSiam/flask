import requests
from .manager import ConnectorManager, BaseConnector


@ConnectorManager.register("servicenow")
class ServiceNowConnector(BaseConnector):

    # ---------------------------------------------------------------------------
    # Core analytical tables for standard ITSM / CMDB data integrations.
    # Modelled after the table sets used by Fivetran, Airbyte, and Google's
    # ServiceNow connector.  Extend this list for custom / HR / CSM modules.
    # ---------------------------------------------------------------------------
    CORE_TABLES = [
        # ITSM – task hierarchy
        "task",
        "incident",
        "problem",
        "change_request",
        "change_task",
        # Service Catalog
        "sc_request",
        "sc_req_item",
        "sc_task",
        # Users & access management
        "sys_user",
        "sys_user_group",
        "sys_user_grmember",
        "sys_user_role",
        "sys_user_has_role",
        # CMDB & asset management
        "cmdb_ci",
        "cmdb_rel_ci",
        "alm_asset",
        "alm_hardware",
        # Organisational reference data
        "cmn_location",
        "cmn_department",
        "core_company",
    ]

    # Mapping from ServiceNow internal_type → friendly type string
    _SN_TYPE_MAP = {
        "string": "string",
        "integer": "integer",
        "boolean": "boolean",
        "glide_date_time": "datetime",
        "glide_date": "date",
        "glide_time": "time",
        "float": "float",
        "decimal": "decimal",
        "long": "integer",
        "reference": "reference",
        "currency": "decimal",
        "url": "string",
        "email": "string",
        "phone_number": "string",
        "GUID": "string",
    }

    # ---------------------------------------------------------------------------
    # Session & connection helpers
    # ---------------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        username = self.config.get('username', '')
        # TODO: Implement credential vault decryption here once ready
        # e.g., plaintext_password = credential_vault.decrypt(self.config.get('password', ''))
        plaintext_password = self.config.get('password', '')

        if username and plaintext_password:
            session.auth = (username, plaintext_password)

        additional = self.config.get('additional_params') or {}

        # Configure additional session parameters if needed
        if 'headers' in additional and isinstance(additional['headers'], dict):
            session.headers.update(additional['headers'])

        if 'verify_ssl' in additional:
            # Can be boolean or path to cert bundle
            session.verify = additional['verify_ssl']

        return session

    @property
    def base_url(self) -> str:
        host = self.config.get('host', '').rstrip('/')
        if not host.startswith('http'):
            host = f"https://{host}"
        return host

    def test_connection(self) -> tuple:
        """Test ServiceNow connection using the Table API."""
        try:
            session = self._build_session()
            url = f"{self.base_url}/api/now/table/sys_user?sysparm_limit=1"
            resp = session.get(url, timeout=10)

            if resp.status_code == 200:
                return True, "Connection successful"
            else:
                return False, f"HTTP {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, str(e)

    # ---------------------------------------------------------------------------
    # Access / entitlement placeholders
    # ---------------------------------------------------------------------------

    def fetch_access_points(self) -> list[dict]:
        """Return normalized access point records for sync."""
        # Placeholder for paginating through sys_user_role / sys_security_acl
        return []

    def fetch_entitlements(self, access_point: dict) -> list[dict]:
        """Return entitlements for a specific access point."""
        # Placeholder for future implementation
        return []

    # ---------------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------------

    def _get_type_map_for_table(
        self,
        session: requests.Session,
        table_name: str,
        column_names: list,
    ) -> dict:
        """
        Query sys_dictionary for the given table to retrieve column type info.

        Returns a dict mapping column_name -> friendly type string.
        Falls back to 'string' for any column not found in the dictionary.

        A single batched request is issued, asking only for element + internal_type
        to keep the payload small.
        """
        type_map = {}

        url = (
            f"{self.base_url}/api/now/table/sys_dictionary"
            f"?sysparm_query=name%3D{table_name}"
            f"&sysparm_fields=element,internal_type"
            f"&sysparm_limit=500"
        )

        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                for row in resp.json().get("result", []):
                    col_name = row.get("element", "")
                    raw_type = row.get("internal_type", {})
                    # internal_type can arrive as {"value": "...", "display_value": "..."}
                    if isinstance(raw_type, dict):
                        raw_type = raw_type.get("value", "string")
                    friendly = self._SN_TYPE_MAP.get(str(raw_type), "string")
                    if col_name:
                        type_map[col_name] = friendly
        except Exception:
            pass  # Silently fall back to string defaults

        return type_map

    # ---------------------------------------------------------------------------
    # BaseConnector interface
    # ---------------------------------------------------------------------------

    def get_datasource_metadata(self) -> dict:
        """
        Returns the curated list of core ServiceNow tables.

        Rather than enumerating all 900+ sys_db_object entries, we return a
        static whitelist of the tables that matter for ITSM/CMDB integrations.
        Tables are grouped under a single 'servicenow' schema to match the
        expected { result: [{ schema, tables }] } contract used by the API layer.
        """
        return {
            "result": [
                {
                    "schema": "servicenow",
                    "tables": list(self.CORE_TABLES)
                }
            ],
            "total_tables": len(self.CORE_TABLES)
        }

    def get_table_columns(self, table_name: str, schema: str = None) -> list:
        """
        Returns full column definitions for *table_name*, including all columns
        inherited from parent tables (e.g. task → incident).

        Strategy
        --------
        1. Fetch a single live record from the Table API.  The JSON keys of
           that record represent every available column, regardless of which
           parent table defined it — solving the inheritance problem.
        2. Query sys_dictionary for type information.
        3. Return the merged result, defaulting any unknown column to 'string'.
        """
        session = self._build_session()

        # --- Step 1: Discover columns via a live record -----------------------
        record_url = (
            f"{self.base_url}/api/now/table/{table_name}"
            f"?sysparm_limit=1&sysparm_display_value=false"
        )
        resp = session.get(record_url, timeout=30)

        if resp.status_code == 404:
            raise ValueError(f"Table '{table_name}' not found in ServiceNow instance")
        if resp.status_code != 200:
            raise ValueError(
                f"Failed to fetch columns for '{table_name}': HTTP {resp.status_code}"
            )

        result = resp.json().get("result", [])
        if not result:
            # Table exists but is empty – fall back to sys_dictionary only
            column_names = []
        else:
            column_names = sorted(result[0].keys())

        # --- Step 2: Enrich with type info from sys_dictionary ----------------
        type_map = self._get_type_map_for_table(session, table_name, column_names)

        # --- Step 3: Build the column list ------------------------------------
        columns = []
        for col_name in column_names:
            columns.append({
                "name": col_name,
                "type": type_map.get(col_name, "string"),
                "nullable": True,   # ServiceNow columns are nullable by convention
                "default": None,
                "primary_key": col_name == "sys_id"
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
        Returns paginated data from a ServiceNow table via the Table REST API.

        ServiceNow uses sysparm_limit / sysparm_offset for pagination and
        returns the total record count in the X-Total-Count response header.
        """
        session = self._build_session()

        url = (
            f"{self.base_url}/api/now/table/{table_name}"
            f"?sysparm_limit={limit}&sysparm_offset={offset}"
            f"&sysparm_display_value=false"
        )

        resp = session.get(url, timeout=30)

        if resp.status_code == 404:
            raise ValueError(f"Table '{table_name}' not found in ServiceNow instance")
        if resp.status_code != 200:
            raise ValueError(
                f"Failed to fetch data for '{table_name}': "
                f"HTTP {resp.status_code}: {resp.text}"
            )

        data = resp.json().get('result', [])
        total_count = int(resp.headers.get('X-Total-Count', len(data)))
        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            "page": (offset // limit) + 1 if limit > 0 else 1,
            "pages": total_pages,
            "total": total_count,
            "result": data
        }
