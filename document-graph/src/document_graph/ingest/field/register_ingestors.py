"""Register field ingestors — registers all field-level ingestor providers."""
# Register all field_level ingestors
from ..ingestors_provider_registry import IngestorProviderRegistry
from .numeric_id_cleanup_ingestor import NumericIdCleanupIngestor

# Register field_level ingestors
IngestorProviderRegistry.register("numeric_id_cleanup", NumericIdCleanupIngestor)