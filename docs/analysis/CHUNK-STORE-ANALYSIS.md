# Chunk Text Externalization: Analysis and Path Forward

**Reference:** [DRAFT] [FEATURE] Pluggable external chunk text store to reduce Neptune Analytics memory usage — [awslabs/graphrag-toolkit#324](https://github.com/awslabs/graphrag-toolkit/issues/324)

## The Real Issue

The issue proposes a `ChunkStore` abstraction to move chunk text out of Neptune graph memory. The analysis is correct, but there's a **simpler path** available via a Neptune Analytics feature that the issue doesn't consider.

---

## Neptune Analytics Memory Model (from AWS docs)

Key facts from AWS documentation:

1. **1 m-NCU = 1 GB memory** — Neptune Analytics stores everything in-memory
2. **Pricing**: Fixed hourly cost per m-NCU. At scale, memory = money.
3. **`GraphStorageUsagePercent`** — CloudWatch metric showing memory pressure
4. **All node properties are in-memory** — even if never queried. Large text properties inflate `GraphSizeBytes` and reduce the effective working set for traversal queries.
5. **You cannot selectively evict properties** — if it's stored as a property, it's in memory.

**Cost calculation (confirmed):**
- 5M chunks × 2KB text = 10GB of graph memory
- 10 additional m-NCUs × ~$0.07/hr = ~$50/month just for chunk text
- At the issue's pricing ($500/month for 10GB), the numbers align with larger m-NCU tiers

---

## The `neptune.read()` Alternative

**AWS added `neptune.read()` in 2024** — a CALL procedure that reads data from S3 at query time:

```cypher
CALL neptune.read({
  source: "s3://bucket/chunks/chunk-{chunkId}.parquet",
  format: "parquet"
})
YIELD row
RETURN row.text AS content
```

This means Neptune Analytics can **natively fetch external data from S3 during query execution** without a custom ChunkStore layer. The retriever could:

1. Store chunk text in S3 (Parquet or CSV) during indexing
2. Store only `chunkId` as a graph property (tiny — a few bytes)
3. At retrieval time, use `neptune.read()` to fetch text from S3

**However, there are limitations:**
- `neptune.read()` takes a single S3 object URI — not a prefix
- It reads the entire file and yields rows — no random access by key
- It's designed for batch analytics (join external data with graph), not single-record lookup

**Verdict on `neptune.read()`:** Not suitable for the single-chunk-fetch pattern (4 read paths that need 1-20 chunk texts). It's for batch analytical queries. The ChunkStore approach is still needed.

---

## What's Actually In Memory

Looking at the `ChunkGraphBuilder` write path:

```python
chunk_property_setters = [
    'chunk.value = params.text'   # ← This is the 2KB text
]
properties_c = {
    'chunk_id': chunk_id,         # ~30 bytes
    'text': node.text             # ~2000 bytes ← THE PROBLEM
}
```

Plus any `chunk_metadata` from external properties. For each of 5M chunks:
- `chunkId`: ~30 bytes (UUID or hash)
- `value` (text): ~2000 bytes
- Edges: EXTRACTED_FROM, MENTIONED_IN, BELONGS_TO (~100 bytes each)

The text property is **98% of the per-chunk memory cost**.

---

## The 4 Read Paths (Verified)

| Location | What it does | When called | Batch size |
|----------|-------------|-------------|------------|
| `keyword_vss_provider.py:81` | `RETURN c.value AS content` | Keyword VSS search | 5-20 chunks |
| `statement_enhancement.py:103` | `node.node.metadata['chunk']['value']` | LLM synthesis | 1 chunk at a time |
| `traversal_based_base_retriever.py:156` | `value: NULL` (explicitly skipped!) | Main retriever | N/A — already external |
| `update_chunk_metadata.py:18` | `chunk.metadata.pop('value', None)` | Post-processing | Already in memory |

**Key insight confirmed:** The main traversal retriever (`traversal_based_base_retriever.py:156`) already projects `value: NULL`. The architecture already treats chunk text as external. The memory is wasted.

---

## Path Forward: Recommended Approach

### Option A: S3 + Batch Fetch (Simplest, Highest ROI)

**Write path:**
- Store chunk text as Parquet files in S3, partitioned by source document
- Store only `chunkId` in Neptune (drop `chunk.value`)
- Key pattern: `s3://{bucket}/chunk-text/{sourceId}/{chunkId}.txt` or batched Parquet

**Read path (only 2 locations actually need text):**
- `keyword_vss_provider.py`: After getting `chunkId` list from graph, batch-fetch from S3
- `statement_enhancement.py`: Single-fetch from S3 by `chunkId`

**No change needed for:**
- `traversal_based_base_retriever.py` — already projects `value: NULL`
- `update_chunk_metadata.py` — would read from the same S3 source

**Estimated complexity:** ~100 lines. No new abstraction needed for the first implementation.

### Option B: ChunkStore Protocol (As Proposed in Issue)

Full abstraction with pluggable backends. More complex but more extensible.

**When to choose Option B over A:**
- If you need DynamoDB (sub-5ms latency) for real-time applications
- If you need Redis (sub-1ms) for high-throughput retrieval
- If multiple teams need different backends

**When Option A is sufficient:**
- S3 GET latency (5-15ms) is negligible vs LLM synthesis (1-5s)
- Single deployment, single team
- Want the memory savings now without protocol design

### Option C: Neptune Database (Not Analytics)

If using Neptune Database (not Analytics), properties are on disk, not in memory. The issue is **specific to Neptune Analytics**. On Neptune DB:
- Properties are stored on SSD
- Memory is used for buffer cache (hot data)
- Large text properties only matter if they evict hot structural data from cache

If the deployment is Neptune DB, this optimization has lower priority.

---

## Recommendation

**Start with Option A** (S3 batch fetch, ~100 lines) as a PR. It solves 95% of the problem:
- Remove `chunk.value` from graph properties → immediate 10GB memory savings
- Add S3 batch fetch in the 2 locations that need text
- Keep it simple: no factory pattern, no protocol, just S3 boto3 calls

**Evolve to Option B** (ChunkStore protocol) only if:
- Multiple backend requirements emerge
- Latency requirements tighten below S3 capabilities
- The pattern needs to be reusable across other property types

The issue's proposed design (Option B) is correct and well-architected, but it's premature for the first cut. Ship the savings first, abstract later.

---

## Neptune Analytics Specific Guidance (from AWS Well-Architected)

From the [Cost Optimization Pillar](https://docs.aws.amazon.com/prescriptive-guidance/latest/neptune-analytics-well-architected-framework/cost-optimization-pillar.html):

> Monitor CloudWatch metrics... `GraphStorageUsagePercent`, `GraphSizeBytes`... to assess whether the provisioned capacity is appropriately sized.

The recommended action is to **monitor `GraphSizeBytes` before and after** removing chunk text:
```
Before: GraphSizeBytes includes 10GB of chunk.value text
After:  GraphSizeBytes reduced by ~10GB → can downsize m-NCU
```

This directly reduces the hourly m-NCU cost.

---

## Summary

| Aspect | Assessment |
|--------|-----------|
| Issue valid? | Yes — chunk text in graph memory is wasteful at scale |
| Neptune Analytics specific? | Yes — DB mode stores on disk, less urgent |
| Proposed solution (ChunkStore)? | Over-engineered for first cut; correct for long-term |
| Recommended first step | S3 batch fetch (~100 lines), remove chunk.value from graph |
| `neptune.read()` applicable? | No — designed for batch analytics, not single-record lookup |
| Key metric to track | `GraphSizeBytes` and `GraphStorageUsagePercent` before/after |
| Latency impact | Negligible (15-30ms S3 vs 1-5s LLM synthesis) |
| Cost impact | ~$50-500/month savings depending on m-NCU tier |

---

© 2024-2026 Kanjani AI Research.
