# Custom Extraction in the Composable Pipeline — Spec & TODOs

Status: DRAFT
Branch: `feature/composable-extraction-pipeline`
Builds on: cherry-picked PR [awslabs/graphrag-toolkit#241](https://github.com/awslabs/graphrag-toolkit/pull/241) (closed/draft upstream), tracked by [issue #240](https://github.com/awslabs/graphrag-toolkit/issues/240)
Related: internal design doc "Customer-Provided / Custom Extraction for Unstructured Data" (Pippin `bXJhj8ZzKiFL4h`)

## 0. Rebase notes

PR #241's 8 commits were cherry-picked onto current `main` and conflicted in
four files with the ontology-guided-extraction feature that landed on `main`
after the PR was opened. Conflicts were resolved additively (both features'
fields/exports kept). One existing test
(`test_ontology_prompt_composition.py::test_a_no_ontology_prompt_is_the_shipped_prompt`)
needed updating because PR 241 bakes a `{schema_constraints}` placeholder
into the shipped `EXTRACT_TOPICS_PROMPT`, which changes that prompt's text —
and therefore its `LLMCache` key — for every existing user, even with no
schema configured. That is the same invariant ("byte-identical prompt for a
non-adopter") the ontology feature's own `with_ontology_constraints` design
was built to preserve for `{ontology_constraints}`. Flagged for reviewers,
not re-litigated here: the two features now coexist, but `schema_constraints`
does not yet get the same byte-identical treatment ontology constraints do.

## 0a. Vocabulary alignment with the retrieval post-processor chain

The retrieval side (`retrieval/processors/`, `retrieval/post_processors/`,
e.g. `StatementEnhancementPostProcessor`) already has an established
error-handling vocabulary for "a pipeline step failed on one item, don't
take the rest down with it": catch inside the step, log **at error level**
naming the operation and the exception (e.g. `"Error enhancing statement:
{e}"`), and degrade by returning that item unchanged.

Extraction's `CustomExtractionStage`/`FallbackExtractionStage` adopt that
logging level and message style directly - failures are now logged with
`logger.error(...)`, not `warning`/`info`, phrased as "Error running X for
node Y: {e}". What is **not** adopted is the retrieval side's "just return
unchanged" semantics in place of the `EXTRACTION_STATUS_KEY` status-tag
contract: the retrieval chain never needs a step to retry via a *different*
step, so it has no equivalent concept to borrow a name from, and removing
the status tag would remove `FallbackExtractionStage`'s ability to move a
failed node to the next stage in the chain. The two chains' vocabularies are
aligned where they describe the same thing (how a failure is logged) and
left distinct where they don't (single-step degrade vs. multi-step retry).

## 1. Goal

PR #241 gave the toolkit a composable extraction pipeline: `ExtractionStage`
(ABC with `input_keys()`/`output_keys()`/`as_transform()`), `PipelineBuilder`
(build-time key-compatibility validation), and `ExtractionConfig.from_stages()`.
That seam lets a pipeline be *assembled* from stages, but every stage shipped
with it is toolkit-owned (LLM proposition/topic extraction, local NER, schema
filter, entity merge).

This spec covers the next increment: letting a user plug **their own**
extraction logic into that seam — an in-house NER service, a deterministic
rule-based extractor, a remote endpoint — without forking the toolkit, while
the pipeline as a whole stays reliable:

- custom extraction logic runs as an ordinary `ExtractionStage`, usable in
  both **batch** and **real-time (synchronous, in-process)** jobs;
- a single document's failure in a custom stage must never block or corrupt
  the rest of the batch ("no partial graph", scoped **per document**, not
  per job);
- a failed document can **fall through a configured chain** of stages
  (e.g. custom → default LLM) and, if it exhausts the chain, is **quarantined**
  rather than silently dropped or allowed to crash the run.

This mirrors R13/R16/R17 from the design doc, scoped to what belongs in the
open-source toolkit (in-process library seam) rather than the AWS-infra
concerns the doc also covers (IAM, Lambda, S3 envelopes, Signer, VPC) — those
are downstream integration concerns for whoever wires a `RemoteExtractionStage`
on top of this seam, not something `graphrag-toolkit` itself needs to know
about.

## 2. Non-goals (this increment)

- No actual remote/Lambda/HTTP transport. That's a `RemoteExtractionStage`
  built *on top of* this spec's stages, out of scope here (tracked as a
  follow-up — see §5, item 6).
- No AWS Signer / supply-chain / IAM concerns — those apply to whoever hosts
  the custom code, not to the toolkit's in-process contract.
- No delegation envelope schema (S3 references, `contentType` enum) — that's
  specific to a remote transport, not the local stage contract.

## 3. Design

### 3.1 `CustomExtractionStage`

A stage that wraps a user-supplied callable as an `ExtractionStage`:

```python
CustomExtractionStage(
    extractor_fn: Callable[[BaseNode], Dict[str, Any]],
    input_keys: List[str] = [],
    output_keys: List[str] = [],
    name: str = 'CustomExtractionStage',
)
```

- `extractor_fn` is called once per node (document/chunk) and must return a
  dict of metadata to merge onto the node.
- A node is the atomic unit of success/failure: if `extractor_fn` raises for
  one node, that failure is caught and recorded on *that node's* metadata
  (status + error), not raised — it never aborts the batch. This is what
  makes per-document fallback (§3.2) and batch fan-out safe: nothing upstream
  of this stage needs try/except of its own.
- Synchronous by construction, so it is usable directly as the "real-time"
  path (one call per document, in-process) and, because `TransformComponent.__call__`
  already takes a `Sequence[BaseNode]`, the exact same stage is the "batch"
  path too — callers that want batched throughput pass a list of many nodes
  through the pipeline in one go, and this increment does not need a
  separate `Batch*` variant the way the LLM stages do.

### 3.2 `FallbackExtractionStage`

A stage that composes an ordered list of `ExtractionStage`s as a fallback
chain:

```python
FallbackExtractionStage(chain: List[ExtractionStage])
```

- Stage 0 runs against every node. Any node left *failed* after stage 0 is
  retried by stage 1, then stage 2, etc. — only the failed subset moves on at
  each step, mirroring the design doc's "only the failed documents re-queue
  to the next stage" (R13).
- A node that fails every stage in the chain is tagged `quarantined` rather
  than raising or being silently dropped. Succeeded nodes are never held up
  by, or corrupted by, documents that end up quarantined.
- A stage with no opinion about status (e.g. an ordinary LLM stage that
  doesn't set a status key) is treated as having succeeded — this is what
  lets `[CustomExtractionStage(...), LLMTopicExtractionStage()]` express
  "try my custom extractor first, fall back to the toolkit default."

### 3.3 Status contract

New constants (`indexing/constants.py`):

| Constant | Value | Meaning |
|---|---|---|
| `EXTRACTION_STATUS_KEY` | `__extraction_status__` | Per-node metadata key a stage may set |
| `EXTRACTION_ERROR_KEY` | `__extraction_error__` | Per-node metadata key for the caught error message |
| `EXTRACTION_STATUS_FAILED` | `failed` | Set by a stage when extraction raised for this node |
| `EXTRACTION_STATUS_QUARANTINED` | `quarantined` | Set by `FallbackExtractionStage` when a node exhausts the chain |

Absence of `EXTRACTION_STATUS_KEY` (or any value other than `failed`) means
success — this keeps existing stages (which know nothing about this
contract) usable as fallback targets unmodified.

## 4. Why TDD, and why this order

Tests first, per the request: each stage above is specified by its test
before it's implemented. The test files below are written to fail (red)
against the current cherry-picked tree, then the implementation is added to
make them pass (green), matching the design doc's own call for "unit +
contract tests (incl. an intentionally failing extractor)" (task 1.2).

## 5. TODO jobs

- [x] **0. Branch + spec** — this document.
- [x] **1. `CustomExtractionStage` (red → green)**
  - Tests (`tests/unit/indexing/extract/test_custom_extraction_stage.py`):
    success path populates node metadata; a raising `extractor_fn` is caught
    per-node (doesn't raise, doesn't affect sibling nodes in the same batch
    call — this *is* the batch-vs-real-time guarantee, since both are the
    same call with a different-sized node list); `input_keys()`/`output_keys()`
    report what was passed in; stage composes through
    `PipelineBuilder`/`ExtractionConfig.from_stages()`.
  - Implementation: `stages/custom_extraction_stage.py`.
- [x] **2. `FallbackExtractionStage` (red → green)**
  - Tests (`tests/unit/indexing/extract/test_fallback_extraction_stage.py`):
    all-succeed chain never touches stage 2+; all-fail-then-succeed chain
    recovers via stage 1; all-stages-fail quarantines the node without
    raising; mixed batch (some nodes succeed at stage 0, some fail and
    recover at stage 1, some exhaust the chain) resolves each node
    independently in one call — the per-document isolation guarantee;
    composes through `PipelineBuilder`; a stage with no opinion about status
    (e.g. a toolkit-default stage) is usable as a fallback target unmodified.
  - Implementation: `stages/fallback_extraction_stage.py`.
- [x] **3. End-to-end composition test**
  - `tests/unit/indexing/extract/test_custom_extraction_pipeline_integration.py`:
    a pipeline built via
    `ExtractionConfig.from_stages([FallbackExtractionStage([CustomExtractionStage(...), CustomExtractionStage(...)])])`
    runs through `PipelineBuilder.build()` and produces the expected mix of
    extracted + recovered output against a small fixture corpus, with each
    document in a mixed-outcome batch resolved independently.
  - Full suite green after this increment: 4204 passed, 1 skipped.
- [x] **3a. Generic per-node isolation for built-in-style stages (gap found on review)**
  - As first committed, `FallbackExtractionTransform` only caught failures
    from stages using its own `EXTRACTION_STATUS_KEY` convention — i.e. only
    `CustomExtractionStage`. Chaining in any shipped built-in stage
    (`NERExtractionStage`, `LLMTopicExtractionStage`, ...), which raises
    outright and has no opinion about that convention, still crashed the
    whole batch. Fixed test-first in
    `TestFallbackExtractionStageIsolatesBuiltinStyleStages`
    (`test_fallback_extraction_stage.py`): a plain raising
    stage/TransformComponent is now isolated per-node by
    `FallbackExtractionTransform` itself. The happy path still calls each
    stage once per batch (no isolation cost when nothing fails); only a
    raising batch call is retried node-by-node to attribute the failure to
    the actual culprit. Full suite green: 4209 passed, 1 skipped.
- [x] **3b. Vocabulary alignment with the retrieval post-processor chain**
  - See §0a. Logging in `CustomExtractionStage`/`FallbackExtractionStage`
    moved from `warning`/`info` to `error`, phrased like the retrieval
    side's ("Error running X for node Y: {e}"); docstrings now name the
    retrieval precedent explicitly. The `EXTRACTION_STATUS_KEY` status-tag
    contract is kept as-is — deliberately not aligned to the retrieval
    side's "just return unchanged," since that contract is what makes
    multi-stage fallback retry possible and the retrieval chain has no
    equivalent to borrow from. No behavior change; log level/text only.
- [~] **4. Batch vs. real-time parity for custom pipelines (partially covered)**
  - `tests/unit/indexing/extract/test_custom_extraction_pipeline_real_time_and_batch.py`:
    - [x] A `from_stages([...])`-style pipeline (built via `PipelineBuilder`)
      runs end-to-end through `ExtractionPipeline`'s real-time
      (`_extract_fixed_batch`) path, including per-document fallback
      recovery and quarantine, confirmed against the real `extract()` call
      (not just the stage's `TransformComponent` in isolation).
    - [x] `ExtractionPipeline._find_batch_config` picks up a `Batch*Stage`
      from a custom pipeline exactly as it already does for the non-custom
      path (confirmed directly against `_batch_config`, not a full Bedrock
      run — a live Bedrock batch run is out of scope for a unit test).
    - [ ] **Open decision, not yet made:** whether mixing a `Batch*Stage` and
      a synchronous `CustomExtractionStage`/`FallbackExtractionStage` in one
      `from_stages([...])` list should be rejected at build time or
      supported. A test pinning the "reject" behavior exists but is
      `@pytest.mark.skip`'d rather than decided unilaterally — see open
      question 1 below. Today this combination is **silently accepted**
      by `PipelineBuilder` (no error either way), which is itself a gap:
      mixing them currently produces whichever of "silently works" or
      "silently wrong" the particular stages happen to fall into, with no
      test pinning which.
- [ ] **5. Documentation**
  - Usage guide section + notebook cell showing a custom extractor with a
    fallback to the default LLM stage (mirrors the design doc's task 1.3).
- [ ] **6. Observability**
  - `FallbackExtractionTransform` already logs one error-level line per
    quarantined document (see §0a); add a per-chain summary (counts
    succeeded/fell-back/quarantined)
    so a user can see fallback activity in aggregate without instrumenting
    their own callable (design doc's `ExtractionFallbackToDefault` metric,
    scoped to a log line since the toolkit has no metrics sink of its own).
- [ ] **7. Follow-up (not in this branch): `RemoteExtractionStage`**
  - A transport-specific stage (HTTP/Lambda/etc.) built on top of
    `CustomExtractionStage`'s contract, for the AWS-infra integration
    described in the Pippin design doc. Needs its own envelope schema and
    is out of scope here (§2).

## 6. Open questions for reviewers

1. Should mixing a `Batch*Stage` and a synchronous `CustomExtractionStage`/
   `FallbackExtractionStage` in one `from_stages([...])` list be supported or
   rejected at build time (item 4)?
2. Scope check: is "custom extraction" here third-party `ExtractionStage`
   subclasses/callables only (what this increment covers), or does it also
   need a built-in HTTP/remote-endpoint transport (item 7) in the same
   branch? The Pippin design doc's title ("customer-provided custom
   extraction for unstructured data") gestures at the latter more broadly
   than this increment's in-process callable model covers.
