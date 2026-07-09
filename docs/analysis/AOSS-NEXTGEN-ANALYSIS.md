# AOSS NextGen Vector Search Collections: Analysis

**Reference:** [FEATURE] Add support for AOSS NextGen vector search collections — [awslabs/graphrag-toolkit#359](https://github.com/awslabs/graphrag-toolkit/issues/359)

---

## Summary

Valid feature request. AOSS NextGen collections reject the `engine` and `method` parameters that the current `opensearch_vector_indexes.py` unconditionally includes. The proposed flag-based approach is correct, low-risk, and follows existing patterns.

---

## The Problem (Verified in Source)

`opensearch_vector_indexes.py` line 253-296 builds index mappings with a `method` block:

```python
# Line 277-282 (nmslib path):
method = {
    "name": "hnsw",
    "space_type": "l2",
    "engine": "nmslib",              # ← NextGen rejects this
    "parameters": {"ef_construction": 256, "m": 48},  # ← and this
}
```

AOSS NextGen (OpenSearch 3.x) removed these parameters — the system determines engine and parameters internally. Passing them returns:

```
illegal_argument_exception: "Field parameter 'engine' is not supported"
```

---

## Why This Matters Now

- **NextGen is AWS's recommended collection type** for new deployments
- Scale-to-zero pricing (Classic doesn't have this)
- GPU-accelerated index builds
- 32x compression by default
- Anyone creating a new AOSS collection via console today gets NextGen by default
- This is a **growing** compatibility issue — more users will hit it over time

---

## Proposed Solution Assessment

| Aspect | Assessment |
|--------|-----------|
| Approach (config flag) | ✅ Correct — matches existing `opensearch_engine` pattern |
| Default (False) | ✅ Non-breaking — existing Classic behavior unchanged |
| Mapping difference | ✅ Correctly identified — no `method` block, `space_type` at field level |
| Bulk ingest | ✅ No changes needed (NextGen supports custom doc IDs) |
| Future removal | ✅ Flag is easy to remove when Classic deprecated |
| Alternative rejected (auto-detect) | ✅ Correct rejection — adds latency, hides intent |
| Alternative rejected (new prefix) | ✅ Correct — `aoss-nextgen://` would confuse users |

---

## Implementation Notes

The natural extension point is at line 253 in `opensearch_vector_indexes.py`, where the code already branches on `opensearch_engine`:

```python
# Current structure:
if GraphRAGConfig.opensearch_engine.lower() == 'faiss':
    # faiss mapping
else:
    # nmslib mapping

# Proposed addition (before existing branches):
if GraphRAGConfig.opensearch_serverless_nextgen:
    # NextGen mapping (no method block)
elif GraphRAGConfig.opensearch_engine.lower() == 'faiss':
    # faiss mapping (unchanged)
else:
    # nmslib mapping (unchanged)
```

The NextGen mapping is simpler than either existing branch:

```python
idx_conf = {
    "settings": {"index": {"knn": True}},
    "mappings": {
        "date_detection": False,
        "properties": {
            embedding_field: {
                "type": "knn_vector",
                "dimension": dimensions,
                "space_type": "l2",
            }
        }
    }
}

# Optional compression
if GraphRAGConfig.opensearch_serverless_nextgen_compression:
    idx_conf["mappings"]["properties"][embedding_field]["compression_level"] = \
        GraphRAGConfig.opensearch_serverless_nextgen_compression
```

---

## Config Changes (follows existing pattern exactly)

The `GraphRAGConfig` class (config.py) already has this exact pattern for `opensearch_engine`:

```python
# Existing pattern (line 293, 1165-1173):
_opensearch_engine: Optional[str] = None

@property
def opensearch_engine(self) -> str:
    if self._opensearch_engine is None:
        self._opensearch_engine = os.environ.get('OPENSEARCH_ENGINE', DEFAULT_OPENSEARCH_ENGINE)
    return self._opensearch_engine

@opensearch_engine.setter
def opensearch_engine(self, opensearch_engine: str) -> None:
    self._opensearch_engine = opensearch_engine
```

Two new properties following the identical pattern:
- `opensearch_serverless_nextgen` (bool, env: `OPENSEARCH_SERVERLESS_NEXTGEN`)
- `opensearch_serverless_nextgen_compression` (Optional[str], env: `OPENSEARCH_SERVERLESS_NEXTGEN_COMPRESSION`)

---

## Complexity and Risk

| Dimension | Rating |
|-----------|--------|
| Lines of code | ~80 (config + mapping branch) |
| Risk to existing users | Zero — flag defaults False |
| Testing requirement | Needs real NextGen AOSS endpoint |
| Reviewer effort | Low — clear pattern, well-scoped |

---

## My Take

This is a **well-written, ready-to-implement** feature request. The proposer clearly:
- Has a NextGen endpoint to test against
- Understands the mapping differences from AWS docs
- Followed the existing code patterns
- Considered and correctly rejected alternatives

The "DRAFT" status is misleading — this is implementation-ready. The only reason to hold would be if AWS is planning a deeper refactor of the OpenSearch integration (e.g., auto-detection in a future version). If not, this should be greenlit.

**If I were reviewing:** Approve the approach, request tests against a real NextGen endpoint, merge.

---

© 2024-2026 Kanjani AI Research.
