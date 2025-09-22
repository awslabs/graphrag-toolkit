[[Home](./)]

## Query Engine

The `ByoKGQueryEngine` is the central orchestrating component that coordinates graph connectors, retrievers, and LLMs to process natural language questions and generate answers from knowledge graphs. It handles the high-level flow of query processing while delegating LLM-specific tasks to the KG Linker.

### Topics

  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Initialization](#initialization)
  - [Query processing](#query-processing)
  - [Cypher-based retrieval](#cypher-based-retrieval)
  - [Multi-strategy retrieval](#multi-strategy-retrieval)
  - [Usage examples](#usage-examples)

### Overview

The `ByoKGQueryEngine` orchestrates the interaction between multiple components to answer questions over knowledge graphs. It supports two main modes of operation:

1. **Cypher-based retrieval** - Uses CypherKGLinker for direct query generation and execution
2. **Multi-strategy retrieval** - Uses KGLinker with multiple retrieval strategies (agentic, path-based, query-based)

The engine can operate with either mode independently or combine both approaches for comprehensive question answering.

### Architecture

The query engine integrates the following components:

- **Graph Store** - Provides access to graph data and schema information
- **Entity Linker** - Links natural language entities to graph nodes
- **Triplet Retriever** - Retrieves relevant triplets using agentic exploration
- **Path Retriever** - Finds and verbalizes paths between entities
- **Graph Query Executor** - Executes structured graph queries
- **KG Linker** - Handles LLM-based entity extraction and query understanding
- **Cypher KG Linker** - Specialized for Cypher query generation (optional)

### Initialization

#### Basic initialization

```python
from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine

# Minimal initialization (uses defaults)
query_engine = ByoKGQueryEngine(graph_store=your_graph_store)
```

#### Full initialization with custom components

```python
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    entity_linker=custom_entity_linker,
    triplet_retriever=custom_triplet_retriever,
    path_retriever=custom_path_retriever,
    graph_query_executor=custom_query_executor,
    llm_generator=custom_llm,
    kg_linker=custom_kg_linker,
    cypher_kg_linker=custom_cypher_linker,
    direct_query_linking=False
)
```

#### Default component initialization

When components are not provided, the engine initializes defaults:

**Entity Linker**: Uses `FuzzyStringIndex` with all graph nodes
```python
from indexing import FuzzyStringIndex
from graph_retrievers import EntityLinker

string_index = FuzzyStringIndex()
string_index.add(graph_store.nodes())
entity_retriever = string_index.as_entity_matcher()
entity_linker = EntityLinker(entity_retriever)
```

**Triplet Retriever**: Uses `AgenticRetriever` with graph traversal
```python
from graph_retrievers import AgenticRetriever, GTraversal, TripletGVerbalizer

graph_traversal = GTraversal(graph_store)
graph_verbalizer = TripletGVerbalizer()
triplet_retriever = AgenticRetriever(
    llm_generator=llm_generator,
    graph_traversal=graph_traversal,
    graph_verbalizer=graph_verbalizer
)
```

**Path Retriever**: Uses `PathRetriever` with path verbalization
```python
from graph_retrievers import PathRetriever, GTraversal, PathVerbalizer

graph_traversal = GTraversal(graph_store)
path_verbalizer = PathVerbalizer()
path_retriever = PathRetriever(
    graph_traversal=graph_traversal,
    path_verbalizer=path_verbalizer
)
```

### Query processing

#### Main query method

```python
def query(self, query: str, iterations: int = 2, cypher_iterations: int = 2) -> Tuple[List[str], List[str]]
```

The `query` method processes questions through the retrieval pipeline and returns retrieved context.

**Parameters:**
- `query` (str): The search query
- `iterations` (int): Number of retrieval iterations for multi-strategy approach (default: 2)
- `cypher_iterations` (int): Number of Cypher generation retries (default: 2)

**Returns:**
- Tuple of (retrieved context, final answers) as lists of strings

#### Query processing flow

1. **Initialize context** - Set up empty context lists and entity tracking
2. **Direct query linking** (optional) - Use semantic similarity for initial entity linking
3. **Cypher-based retrieval** (if CypherKGLinker provided) - Generate and execute Cypher queries
4. **Multi-strategy retrieval** (if KGLinker provided) - Use iterative entity extraction and retrieval
5. **Context aggregation** - Combine results from all strategies

### Cypher-based retrieval

When a `cypher_kg_linker` is provided, the engine performs Cypher-based retrieval:

#### Process flow

1. **Generate Cypher response** - Use CypherKGLinker to generate linking and query artifacts
2. **Execute linking queries** - Process `opencypher-linking` artifacts for entity discovery
3. **Execute main queries** - Process `opencypher` artifacts for answer retrieval
4. **Handle failures** - Provide feedback for failed queries to improve subsequent iterations
5. **Iterate** - Repeat for specified number of `cypher_iterations`

#### Error handling

The engine provides feedback for failed Cypher queries:

```python
if len(answers) == 0:
    cypher_context_with_feedback.append(
        "No executable results for the above. Please improve cypher generation "
        "in the future by focusing more on the given schema and the relations "
        "between node types."
    )
```

### Multi-strategy retrieval

When a `kg_linker` is provided, the engine performs multi-strategy retrieval:

#### Iterative process

1. **Generate LLM response** - Use KGLinker to extract entities, paths, and queries
2. **Link entities** - Connect extracted entities to graph nodes
3. **Retrieve triplets** - Use AgenticRetriever for contextual triplet extraction
4. **Process paths** - Follow extracted metapaths between entities
5. **Execute queries** - Run structured graph queries (Cypher, SPARQL)
6. **Check completion** - Stop if task completion signal is detected

#### Task completion

The engine checks for completion signals in LLM responses:

```python
task_completion = parse_response(response, r"<task-completion>(.*?)</task-completion>")
if "FINISH" in " ".join(task_completion):
    break
```

#### Iterative prompting

The engine uses different prompts for different iterations:
- **First iteration**: Uses standard task prompts
- **Subsequent iterations**: Uses iterative prompts that build on previous context

### Usage examples

#### Basic usage

```python
# Initialize with graph store
query_engine = ByoKGQueryEngine(graph_store=neo4j_store)

# Process a question
question = "What are the side effects of aspirin?"
context = query_engine.query(question)

print("Retrieved context:")
for item in context:
    print(f"- {item}")
```

#### Cypher-focused usage

```python
from graph_connectors import CypherKGLinker

# Initialize with Cypher support
cypher_linker = CypherKGLinker(llm_generator, graph_store)
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    cypher_kg_linker=cypher_linker
)

# Process question with Cypher iterations
question = "Find all drugs that interact with aspirin"
context = query_engine.query(question, cypher_iterations=3)
```

#### Multi-strategy with custom components

```python
# Custom entity linker with semantic similarity
from indexing import SemanticIndex
semantic_index = SemanticIndex(embedding_model)
semantic_index.add(graph_store.nodes())
entity_linker = EntityLinker(semantic_index.as_entity_matcher())

# Initialize with custom components
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    entity_linker=entity_linker,
    direct_query_linking=True  # Enable semantic entity linking
)

# Process with multiple iterations
context = query_engine.query(question, iterations=3)
```

#### Response generation

```python
# Generate final response from retrieved context
question = "What causes headaches?"
context = query_engine.query(question)

# Generate answer using retrieved context
answers, full_response = query_engine.generate_response(
    query=question,
    graph_context="\n".join(context)
)

print("Generated answers:")
for answer in answers:
    print(f"- {answer}")
```

#### Combining both approaches

```python
# Initialize with both KG Linker and Cypher KG Linker
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    kg_linker=kg_linker,
    cypher_kg_linker=cypher_linker
)

# The engine will first try Cypher-based retrieval,
# then fall back to multi-strategy retrieval
context = query_engine.query(question)
```

### Configuration options

#### Direct query linking

Enable semantic similarity-based entity linking:

```python
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    direct_query_linking=True
)
```

#### Custom LLM configuration

```python
from llm import BedrockGenerator

custom_llm = BedrockGenerator(
    model_name='us.anthropic.claude-3-5-sonnet-20240620-v1:0',
    region_name='us-west-2'
)

query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    llm_generator=custom_llm
)
```

#### Iteration control

```python
# Fine-tune iteration counts for different strategies
context = query_engine.query(
    question,
    iterations=4,        # Multi-strategy iterations
    cypher_iterations=2  # Cypher retry iterations
)
