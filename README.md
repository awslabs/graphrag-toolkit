## GraphRAG Toolkit

The graphrag-toolkit is a collection of Python tools for building graph-enhanced Generative AI applications.

Installation instructions and requirements are detailed separately with each tool.

### Lexical Graph

The [lexical-graph](https://github.com/awslabs/graphrag-toolkit/tree/main/lexical-graph) provides a framework for automating the construction of a [hierarchical lexical graph](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/graph-model.md) from unstructured data, and composing question-answering strategies that query this graph when answering user questions.

![Lexical graph](https://github.com/awslabs/graphrag-toolkit/raw/main/images/visualisation.png)

#### Additional Resources

  - [Introducing the GraphRAG Toolkit](https://aws.amazon.com/blogs/database/introducing-the-graphrag-toolkit/) [Blog Post] GraphRAG Toolkit launch blog post.
  - [AWS re:Invent 2025 - Deep Dive into Deloitte's Amazon Neptune GenAI Security Intelligence Center](https://www.youtube.com/watch?v=KD5C93kwBMg) [Video] Discusses the design of the GraphRAG Toolkit and shows how Deloitte use the lexical graph in its security intelligence center.
  - [VectorDB vs GraphDB for Gen AI Agents](https://www.youtube.com/watch?v=qQeB2nuXNNo) [Video] Discussion on the differences between vector search and graph search, how they work, and how to use them together to enhance the accuracy of GenAI applications and more. Includes examples that use the GraphRAG Toolkit.
  - [Leveraging VectorDB and GraphDB: Enhancing Gen AI Applications with Hybrid Queries](https://medium.com/@kwokmeli/leveraging-vectordb-and-graphdb-enhancing-gen-ai-applications-with-hybrid-queries-b3f691b586b2) [Blog Post] Companion blog post to the VectorDB vs GraphDB video above.
  - [Use GraphRAG with Amazon Neptune to improve generative AI applications](https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/workshop) [Code Sample] Jupyter notebook-based self-guided workshop that allows you to explore the GraphRAG Toolkit features. 
  - [RAG Explorer](https://github.com/aws-samples/sample-rag-explorer-graphragtoolkit) [Code Sample] Interactive UI-based app that uses the GraphRAG Toolkit to compare GraphRAG and Vector RAG responses.
  - [Hierarchical Lexical Graph for Enhanced Multi-Hop Retrieval](https://arxiv.org/abs/2506.08074) [Article]  Describes the deisgn of the hierarchical lexical graph model.

### BYOKG-RAG

[BYOKG-RAG](https://github.com/awslabs/graphrag-toolkit/tree/main/byokg-rag) is a novel approach to Knowledge Graph Question Answering (KGQA) that combines the power of Large Language Models (LLMs) with structured knowledge graphs. The system allows users to bring their own knowledge graph and perform complex question answering over it.

## Security

See [CONTRIBUTING](https://github.com/awslabs/graphrag-toolkit/tree/main/CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

