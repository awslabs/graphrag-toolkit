# Copyright (c) Evan Erwee. All rights reserved.

# Core imports
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Document graph imports
# [removed: ai4triage dependency]
# [removed: ai4triage dependency]


def validate_tenant_id(tenant_id: str, fix: bool = False) -> str:
    """
    Validate (and optionally sanitize) a TenantId.

    Rules:
      - Must be 1–10 characters
      - Lowercase alphanumeric only

    Args:
        tenant_id (str): The tenant ID to validate or fix.
        fix (bool): If True, automatically transform into a valid TenantId.
                    Transformation steps:
                        1. Lowercase the string
                        2. Remove all non-alphanumeric chars
                        3. Truncate to 10 characters
                        4. If empty after cleaning, defaults to 'tenant1'

    Returns:
        str: Validated or fixed tenant ID.

    Raises:
        ValueError: If invalid and fix=False.
    """
    if not isinstance(tenant_id, str):
        raise ValueError(f"TenantId must be a string, got {type(tenant_id).__name__}")

    if fix:
        cleaned = re.sub(r'[^a-z0-9]', '', tenant_id.lower())[:10]
        return cleaned if cleaned else "tenant1"

    # strict mode
    if not re.fullmatch(r"[a-z0-9]{1,10}", tenant_id):
        raise ValueError(
            f"Invalid TenantId: '{tenant_id}'. TenantId must be 1–10 characters, lowercase alphanumeric."
        )

    return tenant_id
    
def _load_environment_from_bashrc():
    """Private function to load environment variables from .bashrc"""
    bashrc_path = '/home/ec2-user/.bashrc'
    if os.path.exists(bashrc_path):
        with open(bashrc_path, 'r') as f:
            content = f.read()
        export_pattern = r'export\s+([A-Z_]+)=([^\n]+)'
        matches = re.findall(export_pattern, content)
        for key, value in matches:
            clean_value = value.strip('"\'')
            os.environ[key] = clean_value

def _fetch_neo4j_from_ssm(app_id: Optional[str] = None, region: Optional[str] = None) -> dict:
    """Fetch NEO4J_* from SSM (all SecureString), return decrypted values."""
    import os, boto3, botocore
    app_id = app_id or os.getenv("APPLICATION_ID", "graphrag-toolkit")
    region = region or os.getenv("AWS_REGION", "us-east-1")
    ssm = boto3.client("ssm", region_name=region)

    names = {
        "NEO4J_URI":       f"/{app_id}/database/uri",
        "NEO4J_USERNAME":  f"/{app_id}/database/username",
        "NEO4J_PASSWORD":  f"/{app_id}/database/password",
    }

    out = {}
    for key, name in names.items():
        try:
            # Decrypt everything; safe if the parameter is String too.
            resp = ssm.get_parameter(Name=name, WithDecryption=True)
            val = resp["Parameter"]["Value"].strip()
            out[key] = val
        except botocore.exceptions.ClientError as e:
            # Surface missing/denied params clearly
            raise RuntimeError(f"Failed to get {name}: {e.response['Error']['Code']}") from e
    return out

def build_write_providers(tenant_id: str, root_id: str, document_id: str, owner_id: str,
                         data_dir: Optional[str] = None,
                         enable: tuple = ('graphjson', 'graphcode', 'neo4j', 'neptune', 'falkordb'), 
                         overrides: Optional[Dict] = None, echo: bool = False) -> Dict[str, Any]:
    """Build write providers with proper args structure"""
    
    # Validate data_dir only for file-based providers
    file_providers = {'graphjson', 'graphcode'}
    enabled_file_providers = set(enable) & file_providers
    
    if enabled_file_providers and not data_dir:
        raise ValueError(f"data_dir is required for file-based providers: {enabled_file_providers}")
    
    # Rest of function remains the same...

    """Build write providers with proper args structure"""
    # [removed: ai4triage dependency]
    # [removed: ai4triage dependency]
    # [removed: ai4triage dependency]
    
    _load_environment_from_bashrc()
    
    write_providers = {}
    overrides = overrides or {}
    
    # Base args structure
    base_args = {
        'properties': {
            'tenant_id': tenant_id,
            'root_id': root_id,
            'document_id': document_id,
            'owner': owner_id
        },
        'policies': {}
    }
    
    # GraphJSON
    if 'graphjson' in enable:
        connection_config = overrides.get('graphjson', {}).get('connection_config', {'path': f'{data_dir}/graphjson/graph.json'})
        graphjson_args = base_args.copy()
        # Apply overrides
        graphjson_overrides = overrides.get('graphjson', {})
        if 'args' in graphjson_overrides:
            if 'properties' in graphjson_overrides['args']:
                graphjson_args['properties'].update(graphjson_overrides['args']['properties'])
            if 'policies' in graphjson_overrides['args']:
                graphjson_args['policies'].update(graphjson_overrides['args']['policies'])
        
        config = StorageProviderConfig(
            provider_type='graphjson',
            connection_config=connection_config,
            args=graphjson_args
        )
        write_providers['graphjson'] = StorageProviderFactory.create_storage_provider(config)
        if echo: print(f"✅ GraphJSON provider created: {connection_config['path']}")
    
    # GraphCode
    if 'graphcode' in enable:
        connection_config = overrides.get('graphcode', {}).get('connection_config', 
                                        {'path': f'{data_dir}/graphcode/graph_code.py', 'dialect': 'networkx'})
        graphcode_args = base_args.copy()
        # Apply overrides
        graphcode_overrides = overrides.get('graphcode', {})
        if 'args' in graphcode_overrides:
            if 'properties' in graphcode_overrides['args']:
                graphcode_args['properties'].update(graphcode_overrides['args']['properties'])
            if 'policies' in graphcode_overrides['args']:
                graphcode_args['policies'].update(graphcode_overrides['args']['policies'])
        
        config = StorageProviderConfig(
            provider_type='graphcode',
            connection_config=connection_config,
            args=graphcode_args
        )
        write_providers['graphcode'] = StorageProviderFactory.create_storage_provider(config)
        if echo: print(f"✅ GraphCode provider created: {connection_config['path']}")
    
    #Neo4j (SSM-sourced, no env reliance)
    if 'neo4j' in enable:
        from pydantic import SecretStr
        # [removed: ai4triage dependency]
            ConnectionStringParser,
        )

        def _nonempty(s: Optional[str]) -> bool:
            return isinstance(s, str) and s.strip() != ""

        try:
            creds = _fetch_neo4j_from_ssm()  # {'NEO4J_URI','NEO4J_USERNAME','NEO4J_PASSWORD'}
            neo4j_uri = creds["NEO4J_URI"].strip()
            user = creds["NEO4J_USERNAME"].strip()
            pw_secret = SecretStr(creds["NEO4J_PASSWORD"].strip())

            if _nonempty(neo4j_uri) and _nonempty(user):
                provider_type, config = ConnectionStringParser.make_neo4j_config(
                    neo4j_uri,
                    username=user,
                    password=pw_secret,
                    for_factory=True,  # unwrap to plain str inside connection_string_parser.py
                )

                neo4j_args = {
                    'properties': base_args['properties'].copy(),
                    'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
                }
                # Apply Neo4j overrides
                neo4j_overrides = overrides.get('neo4j', {})
                if 'args' in neo4j_overrides:
                    if 'properties' in neo4j_overrides['args']:
                        neo4j_args['properties'].update(neo4j_overrides['args']['properties'])
                    if 'policies' in neo4j_overrides['args']:
                        neo4j_args['policies'].update(neo4j_overrides['args']['policies'])
                
                storage_config = StorageProviderConfig(
                    provider_type=provider_type,
                    connection_config=config,   # uri + auth=(user, pass) as plain strings
                    args=neo4j_args
                )
                write_providers['neo4j'] = StorageProviderFactory.create_storage_provider(storage_config)
                if echo:
                    print(f"✅ Neo4j provider created: {config['uri']} (auth=yes)")
            else:
                if echo:
                    print("❌ Missing NEO4J_URI or NEO4J_USERNAME from SSM; skipping Neo4j provider")

        except Exception as e:
            if echo:
                print(f"❌ Neo4j init failed via SSM: {e}")

    # Neptune
    if 'neptune' in enable:
        neptune_connection = os.environ.get('DOC_NEP_STORE')
        if neptune_connection:
            provider_type, config = ConnectionStringParser.parse_connection_string(neptune_connection)
            neptune_args = {
                'properties': base_args['properties'].copy(),
                'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
            }
            # Apply Neptune overrides
            neptune_overrides = overrides.get('neptune', {})
            if 'args' in neptune_overrides:
                if 'properties' in neptune_overrides['args']:
                    neptune_args['properties'].update(neptune_overrides['args']['properties'])
                if 'policies' in neptune_overrides['args']:
                    neptune_args['policies'].update(neptune_overrides['args']['policies'])
            
            storage_config = StorageProviderConfig(
                provider_type=provider_type,
                connection_config=config,
                args=neptune_args
            )
            write_providers['neptune'] = StorageProviderFactory.create_storage_provider(storage_config)
            if echo: print(f"✅ Neptune provider created: {neptune_connection}")
    
    # FalkorDB
    if 'falkordb' in enable:
        falkordb_connection = os.environ.get('GRAPH_DOC_STORE')
        if falkordb_connection:
            provider_type, config = ConnectionStringParser.parse_connection_string(falkordb_connection)
            falkordb_args = {
                'properties': base_args['properties'].copy(),
                'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
            }
            # Apply FalkorDB overrides
            falkordb_overrides = overrides.get('falkordb', {})
            if 'args' in falkordb_overrides:
                if 'properties' in falkordb_overrides['args']:
                    falkordb_args['properties'].update(falkordb_overrides['args']['properties'])
                if 'policies' in falkordb_overrides['args']:
                    falkordb_args['policies'].update(falkordb_overrides['args']['policies'])
            
            storage_config = StorageProviderConfig(
                provider_type=provider_type,
                connection_config=config,
                args=falkordb_args
            )
            write_providers['falkordb'] = StorageProviderFactory.create_storage_provider(storage_config)
            if echo: print(f"✅ FalkorDB provider created: {falkordb_connection}")
    
    if echo: print(f"Active write providers: {list(write_providers.keys())}")
    return write_providers

def build_read_providers(tenant_id: str, root_id: str, document_id: str, owner_id: str,
                        data_dir: Optional[str] = None,
                        enable: tuple = ('graphjson', 'graphcode', 'neo4j', 'neptune', 'falkordb'), 
                        overrides: Optional[Dict] = None, echo: bool = False) -> Dict[str, Any]:
    """Build read providers with proper args structure"""
    
    # Validate data_dir only for file-based providers
    file_providers = {'graphjson', 'graphcode'}
    enabled_file_providers = set(enable) & file_providers
    
    if enabled_file_providers and not data_dir:
        raise ValueError(f"data_dir is required for file-based providers: {enabled_file_providers}")
    
    # Rest of function remains the same...

    """Build read providers with proper args structure"""
    # [removed: ai4triage dependency]
    # [removed: ai4triage dependency]
    # [removed: ai4triage dependency]
    
    _load_environment_from_bashrc()
    
    read_providers = {}
    overrides = overrides or {}
    
    # Base args structure
    base_args = {
        'properties': {
            'tenant_id': tenant_id,
            'root_id': root_id,
            'document_id': document_id,
            'owner': owner_id
        },
        'policies': {}
    }
    
    # GraphJSON
    if 'graphjson' in enable:
        connection_config = overrides.get('graphjson', {}).get('connection_config', {'path': f'{data_dir}/graphjson/graph.json'})
        graphjson_args = base_args.copy()
        # Apply overrides
        graphjson_overrides = overrides.get('graphjson', {})
        if 'args' in graphjson_overrides:
            if 'properties' in graphjson_overrides['args']:
                graphjson_args['properties'].update(graphjson_overrides['args']['properties'])
            if 'policies' in graphjson_overrides['args']:
                graphjson_args['policies'].update(graphjson_overrides['args']['policies'])
        
        config = StorageProviderConfig(
            provider_type='graphjson',
            connection_config=connection_config,
            args=graphjson_args
        )
        read_providers['graphjson'] = StorageProviderFactory.create_reader(config)
        if echo: print(f"✅ GraphJSON reader created: {connection_config['path']}")
    
    # GraphCode
    if 'graphcode' in enable:
        connection_config = overrides.get('graphcode', {}).get('connection_config', 
                                        {'path': f'{data_dir}/graphcode/graph_code.py', 'dialect': 'networkx'})
        graphcode_args = base_args.copy()
        # Apply overrides
        graphcode_overrides = overrides.get('graphcode', {})
        if 'args' in graphcode_overrides:
            if 'properties' in graphcode_overrides['args']:
                graphcode_args['properties'].update(graphcode_overrides['args']['properties'])
            if 'policies' in graphcode_overrides['args']:
                graphcode_args['policies'].update(graphcode_overrides['args']['policies'])
        
        config = StorageProviderConfig(
            provider_type='graphcode',
            connection_config=connection_config,
            args=graphcode_args
        )
        read_providers['graphcode'] = StorageProviderFactory.create_reader(config)
        if echo: print(f"✅ GraphCode reader created: {connection_config['path']}")
    
    #Neo4j (SSM-sourced, no env reliance)
    if 'neo4j' in enable:
        from pydantic import SecretStr
        # [removed: ai4triage dependency]
            ConnectionStringParser,
        )

        def _nonempty(s: Optional[str]) -> bool:
            return isinstance(s, str) and s.strip() != ""

        try:
            creds = _fetch_neo4j_from_ssm()  # {'NEO4J_URI','NEO4J_USERNAME','NEO4J_PASSWORD'}
            neo4j_uri = creds["NEO4J_URI"].strip()
            user = creds["NEO4J_USERNAME"].strip()
            pw_secret = SecretStr(creds["NEO4J_PASSWORD"].strip())

            if _nonempty(neo4j_uri) and _nonempty(user):
                provider_type, config = ConnectionStringParser.make_neo4j_config(
                    neo4j_uri,
                    username=user,
                    password=pw_secret,
                    for_factory=True,  # unwrap to plain str inside connection_string_parser.py
                )

                neo4j_args = {
                    'properties': base_args['properties'].copy(),
                    'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
                }
                # Apply Neo4j overrides
                neo4j_overrides = overrides.get('neo4j', {})
                if 'args' in neo4j_overrides:
                    if 'properties' in neo4j_overrides['args']:
                        neo4j_args['properties'].update(neo4j_overrides['args']['properties'])
                    if 'policies' in neo4j_overrides['args']:
                        neo4j_args['policies'].update(neo4j_overrides['args']['policies'])
                
                storage_config = StorageProviderConfig(
                    provider_type=provider_type,
                    connection_config=config,   # uri + auth=(user, pass) as plain strings
                    args=neo4j_args
                )
                read_providers['neo4j'] = StorageProviderFactory.create_reader(storage_config)
                if echo:
                    print(f"✅ Neo4j reader created: {config['uri']} (auth=yes)")
            else:
                if echo:
                    print("❌ Missing NEO4J_URI or NEO4J_USERNAME from SSM; skipping Neo4j reader")

        except Exception as e:
            if echo:
                print(f"❌ Neo4j reader init failed via SSM: {e}")
                
    # Neptune
    if 'neptune' in enable:
        neptune_connection = os.environ.get('DOC_NEP_STORE')
        if neptune_connection:
            provider_type, config = ConnectionStringParser.parse_connection_string(neptune_connection)
            neptune_args = {
                'properties': base_args['properties'].copy(),
                'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
            }
            # Apply Neptune overrides
            neptune_overrides = overrides.get('neptune', {})
            if 'args' in neptune_overrides:
                if 'properties' in neptune_overrides['args']:
                    neptune_args['properties'].update(neptune_overrides['args']['properties'])
                if 'policies' in neptune_overrides['args']:
                    neptune_args['policies'].update(neptune_overrides['args']['policies'])
            
            storage_config = StorageProviderConfig(
                provider_type=provider_type,
                connection_config=config,
                args=neptune_args
            )
            read_providers['neptune'] = StorageProviderFactory.create_reader(storage_config)
            if echo: print(f"✅ Neptune reader created: {neptune_connection}")
    
    # FalkorDB
    if 'falkordb' in enable:
        falkordb_connection = os.environ.get('GRAPH_DOC_STORE')
        if falkordb_connection:
            provider_type, config = ConnectionStringParser.parse_connection_string(falkordb_connection)
            falkordb_args = {
                'properties': base_args['properties'].copy(),
                'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
            }
            # Apply FalkorDB overrides
            falkordb_overrides = overrides.get('falkordb', {})
            if 'args' in falkordb_overrides:
                if 'properties' in falkordb_overrides['args']:
                    falkordb_args['properties'].update(falkordb_overrides['args']['properties'])
                if 'policies' in falkordb_overrides['args']:
                    falkordb_args['policies'].update(falkordb_overrides['args']['policies'])
            
            storage_config = StorageProviderConfig(
                provider_type=provider_type,
                connection_config=config,
                args=falkordb_args
            )
            read_providers['falkordb'] = StorageProviderFactory.create_reader(storage_config)
            if echo: print(f"✅ FalkorDB reader created: {falkordb_connection}")
    
    if echo: print(f"Active read providers: {list(read_providers.keys())}")
    return read_providers

def setup_lexical_graph(
    graph_store_env: str = 'GRAPH_STORE',
    vector_store_env: str = 'VECTOR_STORE',
    graph_type: str = 'neptune',
    echo: bool = False
    ):
    """Setup lexical graph with Neptune/OpenSearch or Neo4j/PostgreSQL"""
    import os
    from urllib.parse import urlparse
    # [removed: ai4triage dependency]
    # [removed: ai4triage dependency]

    _load_environment_from_bashrc()

    try:
        set_logging_config('INFO')

        graph_store_uri = os.environ.get(graph_store_env)  # default path for non-neo4j

        if graph_type == 'neo4j':
            # Register Neo4j factory only when needed
            # [removed: ai4triage dependency]
                Neo4jGraphStoreFactory
            )
            GraphStoreFactory.register(Neo4jGraphStoreFactory)

            # Fetch creds from SSM (decrypted)
            creds = _fetch_neo4j_from_ssm()  # {'NEO4J_URI','NEO4J_USERNAME','NEO4J_PASSWORD'}
            neo4j_uri = creds["NEO4J_URI"].strip()
            user = creds["NEO4J_USERNAME"].strip()
            pw = creds["NEO4J_PASSWORD"].strip()

            # Build authenticated URI: <scheme>://user:pass@host:port[/db]
            p = urlparse(neo4j_uri if '://' in neo4j_uri else f"neo4j+s://{neo4j_uri}")
            scheme = p.scheme or "neo4j+s"
            host = p.hostname or ""
            port = p.port or 7687
            path = p.path or ""  # keep /db if present

            if not host:
                raise RuntimeError("Invalid NEO4J_URI from SSM: missing host")

            graph_store_uri = f"{scheme}://{user}:{pw}@{host}:{port}{path}"

            if echo:
                print("Neo4j registered for lexical graph")

        # Create stores
        graph_store = GraphStoreFactory.for_graph_store(graph_store_uri)
        vector_store = VectorStoreFactory.for_vector_store(os.environ.get(vector_store_env))

        # Create lexical graph index
        lexical_query_engine = LexicalGraphIndex(graph_store, vector_store)

        if echo:
            print("Lexical-graph stores configured")
            print(f"Graph: {type(graph_store).__name__}")
            print(f"Vector: {type(vector_store).__name__}")

        return {
            'graph_store': graph_store,
            'vector_store': vector_store,
            'lexical_query_engine': lexical_query_engine,
            'available': True
        }

    except Exception as e:
        if echo:
            print(f"Lexical graph setup failed: {e}")
        return {
            'graph_store': None,
            'vector_store': None,
            'lexical_query_engine': None,
            'available': False
        }


class HealthChecker:
    """Production health checks for document graph system"""
    
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.health_status = {}
    
    def check_database_connectivity(self):
        try:
            result = self.reader.health_check()
            self.health_status['database_connectivity'] = {
                'status': 'healthy',
                'message': 'Database connection successful'
            }
            return True
        except Exception as e:
            self.health_status['database_connectivity'] = {
                'status': 'unhealthy',
                'error': str(e),
                'message': 'Database connection failed'
            }
            return False
    
    def check_data_integrity(self):
        try:
            orphan_query = "MATCH (n) WHERE NOT (n)--() RETURN count(n) as ORPHAN_COUNT"
            orphan_result = self.reader.retrieve(orphan_query)
            
            missing_props_query = "MATCH (n) WHERE n.tenant_id IS NULL RETURN count(n) as MISSING_TENANT"
            missing_props_result = self.reader.retrieve(missing_props_query)
            
            orphan_count = 0
            missing_tenant_count = 0
            
            if orphan_result and len(orphan_result) > 0:
                result = orphan_result[0]
                if isinstance(result, dict):
                    orphan_count = list(result.values())[0]
            
            if missing_props_result and len(missing_props_result) > 0:
                result = missing_props_result[0]
                if isinstance(result, dict):
                    missing_tenant_count = list(result.values())[0]
            
            issues = []
            if orphan_count > 0:
                issues.append(f"{orphan_count} orphaned nodes")
            if missing_tenant_count > 0:
                issues.append(f"{missing_tenant_count} nodes missing tenant_id")
            
            if issues:
                self.health_status['data_integrity'] = {
                    'status': 'warning',
                    'issues': issues,
                    'message': 'Data integrity issues detected'
                }
            else:
                self.health_status['data_integrity'] = {
                    'status': 'healthy',
                    'message': 'No data integrity issues found'
                }
            
            return len(issues) == 0
            
        except Exception as e:
            self.health_status['data_integrity'] = {
                'status': 'error',
                'error': str(e),
                'message': 'Data integrity check failed'
            }
            return False
    
    def check_performance_baseline(self):
        try:
            start_time = time.time()
            result = self.reader.retrieve("MATCH (n) RETURN count(n) as TOTAL_NODES LIMIT 1")
            duration = time.time() - start_time
            
            if duration < 1.0:
                status = 'healthy'
                message = f'Performance within acceptable range ({duration:.3f}s)'
            elif duration < 5.0:
                status = 'warning'
                message = f'Performance slower than optimal ({duration:.3f}s)'
            else:
                status = 'unhealthy'
                message = f'Performance unacceptably slow ({duration:.3f}s)'
            
            self.health_status['performance_baseline'] = {
                'status': status,
                'response_time': duration,
                'message': message
            }
            
            return status == 'healthy'
            
        except Exception as e:
            self.health_status['performance_baseline'] = {
                'status': 'error',
                'error': str(e),
                'message': 'Performance check failed'
            }
            return False

    def run_full_health_check(self):
        print("Running production health checks...")
        print("=" * 50)
        
        checks = [
            ('Database Connectivity', self.check_database_connectivity, 'database_connectivity'),
            ('Data Integrity', self.check_data_integrity, 'data_integrity'),
            ('Performance Baseline', self.check_performance_baseline, 'performance_baseline')
        ]
        
        all_healthy = True
        
        for check_name, check_func, status_key in checks:
            try:
                is_healthy = check_func()
                status_icon = "✅" if is_healthy else "⚠️" 
                print(f"{status_icon} {check_name}: {self.health_status[status_key]['message']}")
                
                if not is_healthy:
                    all_healthy = False
                    
            except Exception as e:
                print(f"❌ {check_name}: Check failed - {str(e)}")
                all_healthy = False
        
        overall_status = "HEALTHY" if all_healthy else "NEEDS ATTENTION"
        print(f"\nOverall System Status: {overall_status}")
        
        return {
            'overall_status': overall_status,
            'individual_checks': self.health_status,
            'timestamp': datetime.now().isoformat()
        }

def create_storage_providers(connection_string: str, config: Optional[Dict] = None):
    """Create reader and writer storage providers"""
    storage_config = StorageProviderConfig(
        connection_string=connection_string,
        config=config or {}
    )
    
    reader = StorageProviderFactory.create_reader(storage_config)
    writer = StorageProviderFactory.create_storage_provider(storage_config)
    
    return reader, writer

def debug_data_integrity(reader, output_dir: str = "output"):
    """Debug data integrity issues"""
    print("Debugging data integrity issues...")
    print("=" * 40)

    health_report_path = Path(output_dir) / "health_report.json"
    if health_report_path.exists():
        with open(health_report_path, 'r') as f:
            health_data = json.load(f)

        data_integrity_status = health_data['individual_checks']['data_integrity']
        print(f"Data Integrity Status: {data_integrity_status}")

        if 'issues' in data_integrity_status:
            print(f"Issues found: {data_integrity_status['issues']}")

    print("\nManual integrity checks:")

    orphan_query = "MATCH (n) WHERE NOT (n)--() RETURN count(n) as ORPHAN_COUNT"
    orphan_result = reader.retrieve(orphan_query)
    print(f"Orphan query result: {orphan_result}")

    missing_tenant_query = "MATCH (n) WHERE n.tenant_id IS NULL RETURN count(n) as MISSING_TENANT"
    missing_tenant_result = reader.retrieve(missing_tenant_query)
    print(f"Missing tenant query result: {missing_tenant_result}")

    total_nodes_query = "MATCH (n) RETURN count(n) as TOTAL_NODES"
    total_result = reader.retrieve(total_nodes_query)
    print(f"Total nodes: {total_result}")

    sample_nodes_query = "MATCH (n) RETURN n LIMIT 3"
    sample_result = reader.retrieve(sample_nodes_query)
    print(f"Sample nodes: {sample_result}")

def analyze_orphaned_nodes(reader):
    """Analyze orphaned nodes in detail"""
    print("Identifying orphaned nodes...")
    orphaned_nodes_query = "MATCH (n) WHERE NOT (n)--() RETURN n.id as node_id, n.name as name, labels(n) as labels"
    orphaned_result = reader.retrieve(orphaned_nodes_query)

    print("Orphaned nodes (no relationships):")
    for node in orphaned_result:
        if isinstance(node, dict):
            node_id = node.get('node_id', 'unknown')
            name = node.get('name', 'unknown')
            labels = node.get('labels', [])
            print(f"  - {node_id}: {name} ({labels})")

    print("\nExisting relationships:")
    relationships_query = "MATCH (a)-[r]->(b) RETURN a.name as from_name, type(r) as rel_type, b.name as to_name"
    rel_result = reader.retrieve(relationships_query)

    if rel_result:
        for rel in rel_result:
            if isinstance(rel, dict):
                from_name = rel.get('from_name', 'unknown')
                rel_type = rel.get('rel_type', 'unknown')
                to_name = rel.get('to_name', 'unknown')
                print(f"  {from_name} --[{rel_type}]--> {to_name}")
    else:
        print("  No relationships found in the database")

    return orphaned_result, rel_result

def save_health_report(health_report: Dict, output_dir: str = "output"):
    """Save health report to JSON file"""
    health_report_path = Path(output_dir) / "health_report.json"
    health_report_path.parent.mkdir(exist_ok=True)
    
    with open(health_report_path, 'w') as f:
        json.dump(health_report, f, indent=2)
    
    print(f"Health report saved to: {health_report_path}")
    return health_report_path


def clear_database(provider_type: str, tenant_id: str = None, echo: bool = False):
    """Clear database based on provider type - supports document graph and lexical graph"""
    from urllib.parse import urlparse

    _load_environment_from_bashrc()
    
    try:
        if provider_type == 'neptune':
            # Neptune document graph
            # [removed: ai4triage dependency]
            # [removed: ai4triage dependency]
            # [removed: ai4triage dependency]

            connection_string = os.environ.get('DOC_NEP_STORE')
            if not connection_string:
                if echo: print("❌ DOC_NEP_STORE not found")
                return False

            parsed_type, config = ConnectionStringParser.parse_connection_string(connection_string)
            storage_config = StorageProviderConfig(
                provider_type=parsed_type,
                connection_config=config,
                args={
                    'properties': {
                        'tenant_id': tenant_id or 'temp',
                        'root_id': 'temp',
                        'document_id': 'clear-operation',
                        'owner': 'system'
                    },
                    'policies': {'enforce_semver': True, 'semver_invalid_action': 'fix'}
                }
            )

            writer = StorageProviderFactory.create_storage_provider(storage_config)

            if tenant_id:
                if echo: print(f"Clearing Neptune database for tenant: {tenant_id}")
                writer.execute_query(f"MATCH (n)-[r]-() WHERE n.tenant_id = '{tenant_id}' DELETE r")
                writer.execute_query(f"MATCH (n) WHERE n.tenant_id = '{tenant_id}' DELETE n")
            else:
                if echo: print("Clearing entire Neptune database")
                writer.execute_query("MATCH (n)-[r]-() DELETE r")
                writer.execute_query("MATCH (n) DELETE n")

        elif provider_type == 'neptune-lexical':
            # Neptune lexical graph - use smaller batches to avoid timeout
            # [removed: ai4triage dependency]

            connection_string = os.environ.get('GRAPH_STORE')
            if not connection_string:
                if echo: print("❌ GRAPH_STORE not found")
                return False

            graph_store = GraphStoreFactory.for_graph_store(connection_string)

            if echo: print("Clearing Neptune lexical graph database (batch mode)")

            # Clear in small batches to avoid timeout
            batch_size = 100
            total_deleted = 0

            while True:
                try:
                    result = graph_store.execute_query(
                        f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(*) as deleted"
                    )
                    deleted_count = 0
                    if result and len(result) > 0:
                        deleted_count = result[0].get('deleted', 0)

                    total_deleted += deleted_count
                    if deleted_count == 0:
                        break

                    if echo and total_deleted % 500 == 0:
                        print(f"Deleted {total_deleted} nodes so far...")

                except Exception as e:
                    if echo: print(f"Batch deletion stopped at {total_deleted} nodes: {e}")
                    break

            if echo: print(f"Cleared {total_deleted} nodes from Neptune lexical graph")

        elif provider_type == 'neo4j':
            # Neo4j lexical graph - build connection string with auth from SSM
            # [removed: ai4triage dependency]
            # [removed: ai4triage dependency]

            # Register Neo4j factory
            GraphStoreFactory.register(Neo4jGraphStoreFactory)

            try:
                creds = _fetch_neo4j_from_ssm()  # {'NEO4J_URI','NEO4J_USERNAME','NEO4J_PASSWORD'}
                neo4j_uri = creds["NEO4J_URI"].strip()
                neo4j_username = creds["NEO4J_USERNAME"].strip()
                neo4j_password = creds["NEO4J_PASSWORD"].strip()
            except Exception as e:
                if echo: print(f"❌ Failed to fetch Neo4j creds from SSM: {e}")
                return False

            if not neo4j_uri or not neo4j_username or not neo4j_password:
                if echo: print("❌ Missing Neo4j credentials from SSM")
                return False

            # Build authenticated URI: <scheme>://user:pass@host:port[/db]
            p = urlparse(neo4j_uri if '://' in neo4j_uri else f"neo4j+s://{neo4j_uri}")
            scheme = p.scheme or "neo4j+s"
            host = p.hostname or ""
            port = p.port or 7687
            path = p.path or ""   # keep database path if present: /dbname

            if not host:
                if echo: print("❌ Invalid NEO4J_URI: missing host")
                return False

            auth_uri = f"{scheme}://{neo4j_username}:{neo4j_password}@{host}:{port}{path}"

            graph_store = GraphStoreFactory.for_graph_store(auth_uri)

            if tenant_id:
                if echo: print(f"Clearing Neo4j database for tenant: {tenant_id}")
                graph_store.execute_query(f"MATCH (n) WHERE n.tenant_id = '{tenant_id}' DETACH DELETE n")
            else:
                if echo: print("Clearing entire Neo4j database")
                graph_store.execute_query("MATCH (n) DETACH DELETE n")

        elif provider_type == 'opensearch':
            # AWS OpenSearch Serverless - delete indices
            try:
                import boto3
                from opensearchpy import OpenSearch, RequestsHttpConnection
                from aws_requests_auth.aws_auth import AWSRequestsAuth

                connection_string = os.environ.get('VECTOR_STORE')
                if not connection_string or not connection_string.startswith('aoss://'):
                    if echo: print("❌ VECTOR_STORE not found or not OpenSearch Serverless format")
                    return False

                endpoint = connection_string.replace('aoss://', '')
                if endpoint.startswith('https://'):
                    endpoint = endpoint.replace('https://', '')

                region = os.environ.get('AWS_REGION', 'us-east-1')
                service = 'aoss'
                session = boto3.Session()
                creds = session.get_credentials()
                if creds is None:
                    if echo: print("❌ No AWS credentials available for OpenSearch")
                    return False

                awsauth = AWSRequestsAuth(creds, region, service)

                client = OpenSearch(
                    hosts=[{'host': endpoint, 'port': 443}],
                    http_auth=awsauth,
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection
                )

                if echo: print("Clearing OpenSearch Serverless collection")

                indices = client.indices.get_alias("*")
                deleted_indices = []

                for index_name in indices.keys():
                    if not index_name.startswith('.'):  # Skip system indices
                        try:
                            client.indices.delete(index=index_name)
                            deleted_indices.append(index_name)
                            if echo: print(f"Deleted index: {index_name}")
                        except Exception as e:
                            if echo: print(f"Failed to delete index {index_name}: {e}")

                if echo: print(f"Cleared {len(deleted_indices)} indices from OpenSearch Serverless")

            except ImportError:
                if echo: print("❌ Required packages not installed: pip install opensearch-py aws-requests-auth")
                return False

        elif provider_type == 'postgresql':
            if echo: print("Clearing PostgreSQL vector database")
            if echo: print("⚠️ PostgreSQL clearing not implemented - manual deletion required")

        else:
            if echo: print(f"❌ Unsupported provider type: {provider_type}")
            return False

        if echo: print(f"✅ {provider_type} database cleared successfully")
        return True

    except Exception as e:
        if echo: print(f"❌ Failed to clear {provider_type} database: {e}")
        return False

        
# Export functions for nbimport
__all__ = [
    'HealthChecker',
    'create_storage_providers',
    'build_write_providers',
    'build_read_providers',
    'setup_lexical_graph',
    'debug_data_integrity',
    'analyze_orphaned_nodes',
    'save_health_report',
    'clear_database'


]