# Neptune `create_config` TypeError: Analysis and Fix

**Reference:** [BUG] Neptune create_config raises TypeError when read_timeout is passed — [awslabs/graphrag-toolkit#361](https://github.com/awslabs/graphrag-toolkit/issues/361)

---

## The Bug

```python
# neptune_graph_stores.py line 149-155
return Config(
    retries={'total_max_attempts': 1, 'mode': 'standard'},
    read_timeout=600,                    # ← hardcoded default
    user_agent_appid=f'graphrag-lexical-graph-{toolkit_version}',
    **config_args                        # ← user's config unpacked here
)
```

When a user passes `config='{"read_timeout": 5}'`:
- `config_args` = `{"read_timeout": 5}`
- The `**config_args` unpacking produces `read_timeout=5`
- But `read_timeout=600` is already a keyword argument
- Python raises: `TypeError: Config() got multiple values for keyword argument 'read_timeout'`

**The same bug applies to any argument that's both hardcoded AND passed by the user:**
- `read_timeout` (demonstrated)
- `retries` (would also fail if user passes it)
- `user_agent_appid` (would also fail)

---

## Root Cause

The function uses `**config_args` to spread user config, but also hardcodes the same keys as positional kwargs. Python doesn't allow the same keyword to appear twice.

The `setdefault` pattern on line 148 (`config_args.setdefault('max_pool_connections', ...)`) is the **correct** approach — it sets a default only if the user didn't provide one. But `read_timeout`, `retries`, and `user_agent_appid` don't use this pattern.

---

## Fix

**Option A: Use `setdefault` for all hardcoded values (minimal change, backward-compatible)**

```python
def create_config(config: Optional[str] = None):
    toolkit_version = 'unknown'
    try:
        toolkit_version = version('graphrag-lexical-graph')
    except PackageNotFoundError:
        pass

    config_args = {}
    if config:
        config_args = json.loads(config)

    # Defaults — user-provided values take precedence
    config_args.setdefault('max_pool_connections', DEFAULT_MAX_POOL_CONNECTIONS)
    config_args.setdefault('read_timeout', 600)
    config_args.setdefault('retries', {'total_max_attempts': 1, 'mode': 'standard'})
    config_args.setdefault('user_agent_appid', f'graphrag-lexical-graph-{toolkit_version}')

    return Config(**config_args)
```

This is a 4-line diff that:
- Fixes the TypeError
- Allows users to override ANY botocore Config parameter
- Preserves all existing defaults when no config is provided
- Uses the same pattern already established for `max_pool_connections`

**Option B: Pop known keys before spreading (more explicit)**

```python
def create_config(config: Optional[str] = None):
    ...
    config_args = json.loads(config) if config else {}
    config_args.setdefault('max_pool_connections', DEFAULT_MAX_POOL_CONNECTIONS)

    return Config(
        retries=config_args.pop('retries', {'total_max_attempts': 1, 'mode': 'standard'}),
        read_timeout=config_args.pop('read_timeout', 600),
        user_agent_appid=config_args.pop('user_agent_appid', f'graphrag-lexical-graph-{toolkit_version}'),
        **config_args  # remaining user args pass through cleanly
    )
```

This is more explicit about which keys have defaults, but functionally identical.

---

## Impact

**Who's affected:** Anyone using Neptune behind:
- API Gateway (29s timeout)
- ALB (60s idle timeout)
- Lambda (15 min max, but usually configured shorter)
- Step Functions task timeouts
- Any service mesh with request deadlines

The hardcoded 600s `read_timeout` means:
- If Neptune is slow or unresponsive, the client waits **10 minutes** before failing
- Behind a 30s API Gateway, the gateway cuts the connection but the SDK keeps the socket open
- The next request on that pooled connection may hang or get a stale response

Users **need** to configure this, and currently can't.

---

## Why This Is More Than a Config Bug

The real issue is that `read_timeout=600` is an **opinionated default that doesn't match production deployment patterns**:

| Context | Appropriate timeout | Current default |
|---------|--------------------:|----------------:|
| Interactive notebook (Jupyter) | 600s (fine) | 600s ✓ |
| API backend (API Gateway) | 20-25s | 600s ✗ |
| Lambda function | Function timeout - buffer | 600s ✗ |
| Batch indexing job | 300-600s | 600s ✓ |
| Real-time retrieval | 5-10s | 600s ✗ |

The library is designed for notebooks (where 600s makes sense). Production deployments need configurability.

---

## Suggested Path Forward

1. **Fix the bug** (Option A — 4-line diff, non-breaking)
2. **Document the config parameter** in the docs-site configuration page
3. **Consider whether `read_timeout` should differ by operation type:**
   - Indexing (long writes) → 600s default (current)
   - Retrieval (fast reads) → 30s default
   - This would be a separate enhancement, not part of the bug fix

---

## Test

```python
from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import create_config
import json

# Should not raise TypeError:
config = create_config(json.dumps({"read_timeout": 5}))
assert config.read_timeout == 5

# Default still works:
config = create_config()
assert config.read_timeout == 600

# Other overrides work:
config = create_config(json.dumps({"retries": {"total_max_attempts": 3, "mode": "adaptive"}}))
assert config.retries['total_max_attempts'] == 3
```

---

© 2024-2026 Kanjani AI Research.
