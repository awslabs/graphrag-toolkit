
## Using Custom Prompt Providers

The GraphRAG Toolkit supports pluggable prompt providers to allow dynamic loading of prompt templates from various sources. All providers support AWS template integration for structured outputs and seamlessly handle document-graph query results through the `{query}` variable.

### AWS Template Support

All prompt providers support automatic AWS template loading and substitution:
- Use `{aws_template_structure}` placeholder in user prompts
- Templates are automatically loaded from S3 or local files (any format: txt, json, md, etc.)
- Enables structured outputs for compliance and automation

### Document-Graph Integration

The system seamlessly integrates with document-graph queries:
- Document-graph results flow through the `{query}` variable as text
- No special handling required - system is input-agnostic
- Supports complex knowledge graph traversal → RAG → LLM workflows

### System vs User Prompts

The GraphRAG Toolkit uses a two-prompt architecture following LlamaIndex ChatPromptTemplate:

**System Prompt:**
- **Role**: Defines the AI's identity, expertise, and behavior
- **Content**: Instructions on how to act (e.g., "You are an AWS security expert")
- **Purpose**: Sets context, tone, and domain knowledge
- **Variables**: No dynamic variables - static instructions

**User Prompt:**
- **Role**: Contains the actual task and dynamic content
- **Content**: Task instructions with variable placeholders
- **Purpose**: Processes input data and defines output format
- **Variables**: `{query}`, `{search_results}`, `{additionalContext}`, `{aws_template_structure}`

**Example Structure:**
```
System: "You are an AWS security expert specializing in compliance reporting."
User: "Generate evidence report for: {query} using context: {search_results}"
```

---

## Built-in Providers

There are four built-in providers:

### 1. StaticPromptProvider

Use this when your system and user prompts are defined as constants in your codebase.

```python
from graphrag_toolkit.lexical_graph.prompts.static_prompt_provider import StaticPromptProvider

prompt_provider = StaticPromptProvider()
```

This provider uses the predefined constants `ANSWER_QUESTION_SYSTEM_PROMPT` and `ANSWER_QUESTION_USER_PROMPT`. AWS template placeholders are automatically removed if no template is available.

---

### 2. FilePromptProvider

Use this when your prompts are stored locally on disk.

```python
from graphrag_toolkit.lexical_graph.prompts.file_prompt_provider import FilePromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import FilePromptProviderConfig

prompt_provider = FilePromptProvider(
    FilePromptProviderConfig(base_path="./prompts"),
    system_prompt_file="system.txt",
    user_prompt_file="user.txt",
    aws_template_file="aws_template.json"  # optional AWS template (any format)
)
```

The prompt files are read from a directory (`base_path`), and you can override the file names if needed. AWS templates are automatically loaded and substituted into `{aws_template_structure}` placeholders.

---

### 3. S3PromptProvider

Use this when your prompts are stored in an Amazon S3 bucket.

```python
from graphrag_toolkit.lexical_graph.prompts.s3_prompt_provider import S3PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import S3PromptProviderConfig

prompt_provider = S3PromptProvider(
    S3PromptProviderConfig(
        bucket="ccms-prompts",
        prefix="prompts",
        aws_region="us-east-1",        # optional if set via env
        aws_profile="my-profile",      # optional if using default profile
        system_prompt_file="my_system.txt",  # optional override
        user_prompt_file="my_user.txt",      # optional override
        aws_template_file="aws_template.json" # optional AWS template (any format)
    )
)
```

Prompts are loaded using `boto3` and AWS credentials. AWS templates are automatically loaded from S3 and substituted into `{aws_template_structure}` placeholders. Ensure your environment or `~/.aws/config` is configured for SSO, roles, or keys.

---

### 4. BedrockPromptProvider

Use this when your prompts are stored and versioned using Amazon Bedrock prompt ARNs.

```python
from graphrag_toolkit.lexical_graph.prompts.bedrock_prompt_provider import BedrockPromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig

prompt_provider = BedrockPromptProvider(
    config=BedrockPromptProviderConfig(
        system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/my-system",
        user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/my-user",
        system_prompt_version="DRAFT",
        user_prompt_version="DRAFT",
        aws_template_s3_bucket="my-templates",    # optional S3 bucket for templates
        aws_template_s3_key="templates/aws.json"  # optional S3 key for templates (any format)
    )
)
```

This provider resolves prompt ARNs dynamically using STS and can fall back to S3 for AWS template loading. Templates are substituted into `{aws_template_structure}` placeholders.

