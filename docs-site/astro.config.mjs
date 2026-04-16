import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import react from '@astrojs/react';

// `site` and `base` together determine the public URL.
// For the upstream awslabs project page:  site='https://awslabs.github.io', base='/graphrag-toolkit'
// For a fork's project page:               site='https://<user>.github.io',  base='/<repo-name>'
export default defineConfig({
  site: 'https://awslabs.github.io',
  base: '/graphrag-toolkit',
  integrations: [
    react(),
    starlight({
      title: 'GraphRAG Toolkit',
      description:
        'Documentation for the AWS GraphRAG Toolkit — lexical-graph and BYOKG-RAG.',
      logo: { src: './src/assets/logo.svg' },
      customCss: ['./src/styles/custom.css'],
      social: {
        github: 'https://github.com/awslabs/graphrag-toolkit',
      },
      head: [
        {
          tag: 'link',
          attrs: {
            rel: 'stylesheet',
            href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap',
          },
        },
      ],
      sidebar: [
        {
          label: 'Lexical Graph',
          items: [
            { label: 'Overview', slug: 'lexical-graph/overview' },
            { label: 'Graph Model', slug: 'lexical-graph/graph-model' },
            { label: 'Storage Model', slug: 'lexical-graph/storage-model' },
            {
              label: 'Indexing',
              items: [
                { label: 'Indexing', slug: 'lexical-graph/indexing' },
                { label: 'Batch Extraction', slug: 'lexical-graph/batch-extraction' },
                { label: 'Configuring Batch Extraction', slug: 'lexical-graph/configuring-batch-extraction' },
                { label: 'Versioned Updates', slug: 'lexical-graph/versioned-updates' },
                { label: 'Metadata Filtering', slug: 'lexical-graph/metadata-filtering' },
                { label: 'Reader Providers', slug: 'lexical-graph/readers' },
                { label: 'External Properties', slug: 'lexical-graph/external-properties' },
              ],
            },
            {
              label: 'Querying',
              items: [
                { label: 'Querying', slug: 'lexical-graph/querying' },
                { label: 'Traversal-Based Search', slug: 'lexical-graph/traversal-based-search' },
                { label: 'Traversal-Based Search Configuration', slug: 'lexical-graph/traversal-based-search-configuration' },
                { label: 'Semantic-Guided Search', slug: 'lexical-graph/semantic-guided-search' },
              ],
            },
            {
              label: 'Graph Stores',
              items: [
                { label: 'Neptune Analytics', slug: 'lexical-graph/graph-store-neptune-analytics' },
                { label: 'Neptune Database', slug: 'lexical-graph/graph-store-neptune-db' },
                { label: 'Neo4j', slug: 'lexical-graph/graph-store-neo4j' },
                { label: 'FalkorDB', slug: 'lexical-graph/graph-store-falkor-db' },
              ],
            },
            {
              label: 'Vector Stores',
              items: [
                { label: 'Neptune Analytics', slug: 'lexical-graph/vector-store-neptune-analytics' },
                { label: 'OpenSearch Serverless', slug: 'lexical-graph/vector-store-opensearch-serverless' },
                { label: 'Postgres', slug: 'lexical-graph/vector-store-postgres' },
                { label: 'S3 Vectors', slug: 'lexical-graph/vector-store-s3-vectors' },
              ],
            },
            { label: 'Configuration', slug: 'lexical-graph/configuration' },
            { label: 'Multi-Tenancy', slug: 'lexical-graph/multi-tenancy' },
            { label: 'Custom Prompts', slug: 'lexical-graph/prompts' },
            { label: 'Security', slug: 'lexical-graph/security' },
            { label: 'Hybrid Deployment', slug: 'lexical-graph/hybrid-deployment' },
            { label: 'AWS Profile Configuration', slug: 'lexical-graph/aws-profile' },
            { label: 'Nova 2 Model Support', slug: 'lexical-graph/nova-2-model-support' },
            { label: 'FAQ', slug: 'lexical-graph/faq' },
          ],
        },
        {
          label: 'BYOKG-RAG',
          items: [
            { label: 'Overview', slug: 'byokg-rag/overview' },
            { label: 'Indexing', slug: 'byokg-rag/indexing' },
            { label: 'Query Engine', slug: 'byokg-rag/query-engine' },
            { label: 'Graph Retrievers', slug: 'byokg-rag/graph-retrievers' },
            { label: 'Multi-Strategy Retrieval', slug: 'byokg-rag/multi-strategy-retrieval' },
            { label: 'Configuration', slug: 'byokg-rag/configuration' },
            { label: 'FAQ', slug: 'byokg-rag/faq' },
          ],
        },
      ],
    }),
  ],
});
