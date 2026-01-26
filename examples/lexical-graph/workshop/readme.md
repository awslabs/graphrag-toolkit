# Use GraphRAG with Amazon Neptune to improve generative AI applications

Retrieval Augmented Generation (RAG) applications use the power of generative AI to analyze private datasets. GraphRAG combines knowledge graphs with RAG to produce explainable responses that are grounded in the semantic relationships between concepts, entities, and the underlying content. Learn how to use the [AWS GraphRAG Toolkit](https://github.com/awslabs/graphrag-toolkit) to transform unstructured and semi-structured data into a catalog of domain-specific GraphRAG tools that can be used in RAG applications and agentic workflows.

The workshop assumes familiarity with graph concepts, vector similarity search, and retrieval augmented generation (RAG) techniques.

## Installation

  1. Create a new AWS CloudFormation stack using the `graphrag-toolkit-workshop.json` template. Supply a unique `ApplicationId` parameter (this parameter is used to name all resources created by the stack).
  2. Once the stack is complete, click the `NeptuneSagemakerNotebook` URI in the **Outputs** tab.
  3. Open notebook `0-Start-Here.ipynb`
  
## Exercises

In this workshop you are going to learn how to build a graph-enhanced generative AI question-answering solution using the AWS GraphRAG Toolkit open source library.

### Exercise 1 – Indexing

In this exercise you learn how to build a graph and vector index from source documents using the GraphRAG Toolkit. 

During the exercise you will:

 - Learn about the Extract and Build stages of the indexing process
 - Inspect the extracted data
 - Learn about the specific graph model used by the toolkit
 - Visualise the resulting graph

### Exercise 2 – Querying

In this exercise you learn how to query data in the toolkit's graph and vector stores. The queries in this exercise show how the graph and vector stores can help answer complex questions that require both:

  - Information that is similar to the question being asked
  - Information that is structurally relevant, but _dissimilar_ to the question being asked
  
During the exercise you will:

 - Compare the results produced by traditional similarity search with those produced by graph-enhanced search
 - Visualise and inspect the results produced by the graph-enhanced search
 - Learn about some of the techniques employed by the graph-enhanced search to improve responses
  
### Exercise 3 - Agentic Use Cases

This exercise shows how the indexing and querying capabilities in the previous exercises can be composed into higher-level agentic solutions, whereby an AI agent orchestrates multiple question-answering interactions to help answer more complex questions.

During the exercise you will:

  - Inspect the graph schemas for the two different datasets used in the exercise
  - Build an MCP server and client
  - Inspect the tool descriptions created by the toolkit
  - Create an agent that utilises the tools to answer some complex questions