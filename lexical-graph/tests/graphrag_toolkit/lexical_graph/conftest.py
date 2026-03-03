import pytest
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator


@pytest.fixture
def default_tenant():
    '''
    Fixture for default tenant ID.
    '''
    return TenantId()


@pytest.fixture
def custom_tenant():
    '''
    Fixture for custom tenant ID.
    '''
    return TenantId("acme")


@pytest.fixture
def default_id_gen(default_tenant):
    '''
    Fixture for default ID generator (backward compatible mode, no delimiter).
    '''
    return IdGenerator(tenant_id=default_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)


@pytest.fixture
def default_id_gen_with_delimiter(default_tenant):
    '''
    Fixture for ID generator with delimiter enabled (collision-resistant mode).
    '''
    return IdGenerator(tenant_id=default_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=True)


@pytest.fixture
def custom_id_gen(custom_tenant):
    '''
    Fixture for custom ID generator (backward compatible mode, no delimiter).
    '''
    return IdGenerator(tenant_id=custom_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)
