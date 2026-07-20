# ADR-0007: Non-Bedrock Model Providers and In-AWS Hosting

- **Status:** Proposed — client decision required
- **Date:** 2026-06-29
- **Stance:** We propose and inform; the client decides (ADR-0001 §1).
- **Relates to:** ADR-0001 (D2, D6), ADR-0004, ADR-0005, requirements R1/R2

## 1. Context
The graphrag-toolkit is AWS-native: Neptune has no public access, so the toolkit
runtime executes inside AWS. Model configuration *defaults* to Amazon Bedrock. The
question is whether the toolkit can use non-Bedrock models (the client runs a local
Ollama stack: a generation model, Mistral 7B, and an Ollama embedding model such as
nomic-embed-text).

Verified in `config.py`:
- `EmbeddingType = Union[BaseEmbedding, str]`; `LLMType = Union[LLM, str]`.
- `to_embedding_model()` returns the instance unchanged when passed a `BaseEmbedding`
  ("if isinstance(embed_model, BaseEmbedding): return embed_model"); it only constructs
  `BedrockEmbedding` for a STRING input. `to_llm()` mirrors this for `LLM`.
- The vector index embeds via `self.embed_model` (the `BaseEmbedding` abstraction), at
  build and at query time.

Conclusion: Bedrock is the string-shorthand default, not a hard coupling. The embedding
seam is already provider-neutral.

## 2. Decision
1. **Non-Bedrock providers are supported today via instance injection.** Inject a
   `BaseEmbedding` (e.g. `OllamaEmbedding(model_name="nomic-embed-text", base_url=...)`)
   into `GraphRAGConfig.embed_model`, and an `LLM` into the generation seam. No Bedrock
   Custom Model Import is required — and for an embedding model it is generally not an
   applicable import target.
2. **Host the model in AWS, reachable by the private-Neptune runtime.** Preferred:
   self-host the same model (Ollama on EC2/ECS/EKS, or a SageMaker endpoint) inside the
   AWS VPC, so build-time and query-time embedding calls stay in-region. Avoid calling a
   client-local model across the network from AWS (latency, throughput, reachability).
3. **Do not rely on bring-your-own-vector** (ADR-0005): the toolkit embeds with the same
   injected model at build and query, guaranteeing consistency.
4. **The embedding seam needs no rework; the enricher does.** The generation/enrichment
   path is vendor-coupled today and must be made model-agnostic to consume the injected
   `LLM` (requirements R1). The embedding path is already agnostic.

## 3. The strategic choice (client-owned)
| Path | What | Trade-off |
|---|---|---|
| (a) Self-managed in AWS | Run the client's exact models (Ollama) on AWS compute | Preserves model + vector size; fastest migration; the client operates model servers |
| (b) Bedrock-native | Use Bedrock embeddings/LLMs (Titan/Cohere, Claude) | Fully managed; changes the model → re-embed + re-tune enrichment |

Both are cloud deployments. (a) is IaaS (self-managed, in-region); (b) is managed/serverless.

## 4. Consequences
- Provider choice is a configuration/injection decision, not a code change (embeddings).
- If models are hosted in AWS, there is no cross-network model dependency; throughput of
  the in-AWS model servers must be sized for batch embedding across many projects.
- Optional upstream OCP enhancement (ADR-0001 D6): extend `to_llm`/`to_embedding_model`
  string resolution to recognise non-Bedrock providers, so string config (not only
  instance injection) works.

## 5. Open questions for the client
1. Exact embedding model and dimension (set `embed_dimensions` to match).
2. Self-managed-in-AWS (a) or Bedrock-native (b)?
3. If self-managed, where hosted and expected batch throughput/latency?