# Nova 2 Model Support in Lexical Graph

## Overview

This document explains how the lexical-graph toolkit supports Amazon Nova 2 series models in AWS Bedrock, including the architecture, implementation details, and usage patterns.

## Background

### The Problem

Amazon Nova 2 series models (Lite, Micro, Pro, Premier, Pro Preview) were released after LlamaIndex's `BedrockConverse` class was implemented. LlamaIndex maintains a hardcoded list of supported models in `llama_index/llms/bedrock_converse/utils.py`, and Nova 2 models are not included in this list. This causes model validation to fail when attempting to use Nova 2 models.

Additionally, Nova 2 models require using inference profile format (e.g., `us.amazon.nova-2-lite-v1:0`) instead of direct model IDs for on-demand throughput, which adds another layer of complexity.

### The Solution

Rather than waiting for LlamaIndex to update their model list or monkey-patching their validation logic, we implemented a custom `DirectBedrockLLM` class that:

1. Uses boto3's `bedrock-runtime` client directly, bypassing LlamaIndex's model validation
2. Implements LlamaIndex's `LLM` interface for compatibility with existing code
3. Properly handles credential management through `GraphRAGConfig.session`
4. Supports pickling for multiprocessing workflows

## Architecture

### Component Overview

```
GraphRAGConfig
    ├── _to_llm() method
    │   ├── Checks if model is in NOVA_2_MODELS list
    │   ├── If yes → DirectBedrockLLM
    │   └── If no → BedrockConverse (LlamaIndex)
    │
    └── session property
        └── Provides boto3 session for AWS authentication
            ├── IRSA in EKS (IAM Roles for Service Accounts)
            └── SSO locally (AWS profiles)

DirectBedrockLLM
    ├── Implements LLM interface
    ├── Uses boto3 bedrock-runtime client
    ├── Gets credentials from GraphRAGConfig.session
    └── Supports pickling via __getstate__/__setstate__
```

### Decision Logic

The `_to_llm()` method in `GraphRAGConfig` determines which LLM implementation to use:

**DirectBedrockLLM is used when:**
- Model ID is in the `NOVA_2_MODELS` list
- Includes both model ID format (`amazon.nova-2-*`) and inference profile format (`us.amazon.nova-2-*`)

**BedrockConverse (LlamaIndex) is used for:**
- All other Bedrock models (Claude, Titan, Cohere, etc.)
- Any model NOT in the `NOVA_2_MODELS` list

## Implementation Details

### Supported Nova 2 Models

The following Nova 2 models are supported (defined in `config.py`):

```python
NOVA_2_MODELS = [
    # Model IDs
    'amazon.nova-2-lite-v1:0',
    'amazon.nova-2-micro-v1:0',
    'amazon.nova-2-pro-v1:0',
    'amazon.nova-2-premier-v1:0',
    'amazon.nova-2-pro-preview-20251202-v1:0',
    # Inference profile formats (required for on-demand throughput)
    'us.amazon.nova-2-lite-v1:0',
    'us.amazon.nova-2-micro-v1:0',
    'us.amazon.nova-2-pro-v1:0',
    'us.amazon.nova-2-premier-v1:0',
    'us.amazon.nova-2-pro-preview-20251202-v1:0',
]
```

### DirectBedrockLLM Class

Located in `lexical-graph/src/graphrag_toolkit/lexical_graph/bedrock_llm.py`:

**Key Features:**

1. **LlamaIndex Compatibility**: Implements the `LLM` interface from LlamaIndex
2. **Credential Management**: Gets boto3 session from `GraphRAGConfig.session`
3. **Pickling Support**: Excludes client from pickle, recreates on unpickle
4. **Lazy Client Creation**: Client property creates client on-demand from session

**Pickling Implementation:**

```python
def __getstate__(self):
    """Exclude client from pickle - will be recreated from GraphRAGConfig.session"""
    state = self.__dict__.copy()
    state['_client'] = None
    return state

def __setstate__(self, state):
    """Restore state and recreate client from GraphRAGConfig.session"""
    self.__dict__.update(state)
    self._client = None  # Will be lazily created via property

@property
def client(self):
    """Lazy client creation from GraphRAGConfig.session"""
    if self._client is None:
        from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        self._client = GraphRAGConfig.session.client('bedrock-runtime')
    return self._client
```

This approach ensures:
- Client is not pickled (which would fail)
- Client is recreated with proper credentials after unpickling
- Works seamlessly in multiprocessing environments

### Configuration Integration

The `_to_llm()` method in `GraphRAGConfig` handles model selection:

```python
def _to_llm(self, llm: LLMType):
    if isinstance(llm, LLM):
        return llm

    # ... session setup ...

    if _is_json_string(llm):
        config = json.loads(llm)
        model_id = config['model']
        
        # Check if this is a Nova 2 model
        if model_id in NOVA_2_MODELS:
            from graphrag_toolkit.lexical_graph.bedrock_llm import DirectBedrockLLM
            logger.info(f"Using DirectBedrockLLM for Nova 2 model: {model_id}")
            return DirectBedrockLLM(
                model=model_id,
                temperature=config.get('temperature', 0.0),
                max_tokens=config.get('max_tokens', 4096)
            )
        
        # Use BedrockConverse for other models
        return BedrockConverse(...)
    
    else:
        # Check if this is a Nova 2 model
        if llm in NOVA_2_MODELS:
            from graphrag_toolkit.lexical_graph.bedrock_llm import DirectBedrockLLM
            logger.info(f"Using DirectBedrockLLM for Nova 2 model: {llm}")
            return DirectBedrockLLM(
                model=llm,
                temperature=0.0,
                max_tokens=4096
            )
        
        # Use BedrockConverse for other models
        return BedrockConverse(...)
```

## Usage

### Explicit Import and Instantiation

To use Nova 2 multimodal embeddings, you must explicitly import and instantiate the class:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding

GraphRAGConfig.embed_model = Nova2MultimodalEmbedding('amazon.nova-2-multimodal-embeddings-v1:0')
GraphRAGConfig.embed_dimensions = 3072
```

### Advanced Configuration

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding

embedding = Nova2MultimodalEmbedding(
    model_name='amazon.nova-2-multimodal-embeddings-v1:0',
    embed_dimensions=3072,
    embed_purpose='TEXT_RETRIEVAL',
    truncation_mode='END'
)

GraphRAGConfig.embed_model = embedding
GraphRAGConfig.embed_dimensions = 3072
```

## IAM Permissions

### Cross-Region Bedrock Access

Nova 2 models use inference profiles which require specific IAM permissions:

```python
# In infrastructure/platform/stacks/argo_workflow_access_stack.py
iam.PolicyStatement(
    effect=iam.Effect.ALLOW,
    actions=[
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
    ],
    resources=[
        # Inference profiles (without account ID)
        "arn:aws:bedrock:*::inference-profile/*",
        # Inference profiles (with account ID)
        f"arn:aws:bedrock:*:{account}:inference-profile/*",
        # Specific inference profile
        f"arn:aws:bedrock:us-east-1::inference-profile/us.amazon.nova-2-lite-v1:0",
        # Foundation models
        f"arn:aws:bedrock:*::foundation-model/*",
    ]
)
```

### Why Both ARN Patterns?

AWS Bedrock inference profiles can have ARNs with or without account IDs:
- `arn:aws:bedrock:*::inference-profile/*` - Cross-account inference profiles
- `arn:aws:bedrock:*:{account}:inference-profile/*` - Account-specific inference profiles

Including both ensures compatibility with all inference profile types.

## Credential Management

### Local Development (SSO)

```bash
# Login to AWS SSO
aws sso login --profile master

# Set profile
export AWS_PROFILE=master
export AWS_REGION=us-east-1

# Run extraction
python extract_script.py
```

### EKS (IRSA)

In EKS, the service account is annotated with an IAM role:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: argo-workflows-server
  namespace: argo-workflows
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::188967239867:role/ArgoWorkflowAccessRole
```

The `GraphRAGConfig.session` automatically uses IRSA credentials when running in EKS.

## Validation

### Successful Workflow Example

```bash
# Submit test workflow
argo submit infrastructure/argo-workflows/templates/extract-bee-test-workflow.yaml \
  -n argo-workflows \
  --watch

# Check logs
argo logs extract-bee-test-6z8nx -n argo-workflows

# Output shows:
# [GraphRAGConfig] Using DirectBedrockLLM for Nova 2 model: us.amazon.nova-2-lite-v1:0
# Successfully extracted 22 JSON files
```

### Verification Steps

1. **Check model selection**: Look for log message indicating DirectBedrockLLM usage
2. **Verify output**: Check S3 for extracted JSON files
3. **Validate credentials**: Ensure no authentication errors in logs
4. **Test pickling**: Verify multiprocessing works without serialization errors

## Comparison: Before vs After

### Previous Implementation (Problematic)

**Issues:**
- Client injection hacks in `llm_cache.py`
- Manual boto3 client creation bypassing proper credential management
- Monkey-patching to work around pickling issues
- Didn't respect IRSA/SSO authentication
- Fragile and hard to maintain

### Current Implementation (Clean)

**Benefits:**
- Clean separation of concerns
- Each LLM class manages its own client
- Proper credential management through `GraphRAGConfig.session`
- No hacks or workarounds
- Proper pickling support via `__getstate__`/`__setstate__`
- Works seamlessly with IRSA in EKS and SSO locally
- Extensible - easy to add more models or custom LLM implementations
- Maintainable architecture

## Adding New Models

To add support for new models that aren't in LlamaIndex's supported list:

1. **Add to NOVA_2_MODELS list** (or create a new list):

```python
# In config.py
NEW_MODELS = [
    'amazon.new-model-v1:0',
    'us.amazon.new-model-v1:0',
]
```

2. **Update _to_llm() logic**:

```python
if model_id in NOVA_2_MODELS or model_id in NEW_MODELS:
    return DirectBedrockLLM(...)
```

3. **Update IAM permissions** if needed:

```python
resources=[
    f"arn:aws:bedrock:*::inference-profile/us.amazon.new-model-v1:0",
]
```

## Troubleshooting

### Model Not Found Error

**Symptom**: `ValueError: Model 'amazon.nova-2-lite-v1:0' is not supported`

**Solution**: Ensure model is in `NOVA_2_MODELS` list and you're using the inference profile format (`us.amazon.nova-2-lite-v1:0`)

### Pickling Errors

**Symptom**: `TypeError: cannot pickle 'botocore.client.BedrockRuntime' object`

**Solution**: Verify `DirectBedrockLLM` is being used (check logs for "Using DirectBedrockLLM" message)

### Authentication Errors

**Symptom**: `UnauthorizedOperation` or `AccessDenied`

**Solution**: 
- Local: Run `aws sso login --profile master`
- EKS: Verify IAM role has correct permissions and service account annotation

### Cross-Region Access Denied

**Symptom**: `AccessDenied` when using inference profiles

**Solution**: Ensure IAM policy includes both ARN patterns:
- `arn:aws:bedrock:*::inference-profile/*`
- `arn:aws:bedrock:*:{account}:inference-profile/*`

## Files Modified

### Core Implementation
- `lexical-graph/src/graphrag_toolkit/lexical_graph/bedrock_llm.py` - NEW: DirectBedrockLLM class
- `lexical-graph/src/graphrag_toolkit/lexical_graph/config.py` - UPDATED: Model selection logic
- `lexical-graph/src/graphrag_toolkit/lexical_graph/__init__.py` - UPDATED: Export DirectBedrockLLM
- `lexical-graph/src/graphrag_toolkit/lexical_graph/utils/llm_cache.py` - FIXED: Removed client injection hack
- `lexical-graph/src/graphrag_toolkit/lexical_graph/utils/bedrock_patch.py` - DELETED: Obsolete monkey-patch approach

### Infrastructure
- `infrastructure/platform/stacks/argo_workflow_access_stack.py` - UPDATED: IAM permissions
- `infrastructure/argo-workflows/templates/extract-bee-test-workflow.yaml` - UPDATED: Use Nova 2 model
- `infrastructure/post-deployment/scripts/images/refresh-lexical-graph-bee.sh` - Build script

## References

- [AWS Bedrock Inference Profiles](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html)
- [LlamaIndex Bedrock Integration](https://docs.llamaindex.ai/en/stable/examples/llm/bedrock/)
- [IRSA Documentation](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)

## Conclusion

The Nova 2 model support implementation provides a clean, maintainable solution for using Amazon's latest models in the lexical-graph toolkit. By implementing a custom LLM class that bypasses LlamaIndex's model validation while maintaining compatibility with the LlamaIndex interface, we achieve:

- Full support for Nova 2 series models
- Proper credential management (IRSA/SSO)
- Multiprocessing compatibility
- Clean architecture without hacks
- Easy extensibility for future models

This approach is significantly better than the previous implementation and provides a solid foundation for supporting new Bedrock models as they are released.
