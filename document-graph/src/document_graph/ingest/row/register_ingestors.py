"""Register row ingestors — registers all row-level ingestor providers."""
# Register all row_level ingestors
from ..ingestors_provider_registry import IngestorProviderRegistry
from .skip_row import SkipRowProvider
from .date_range_filter import DateRangeFilterProvider

# Register row_level ingestors
IngestorProviderRegistry.register("skip_row", SkipRowProvider)
IngestorProviderRegistry.register("date_range_filter", DateRangeFilterProvider)