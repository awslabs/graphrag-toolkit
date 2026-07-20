# Lexical-Graph Logging: File Handler Crashes on Read-Only Filesystems

**Status:** Bug confirmed and fully characterized. No existing issue found.
**Severity:** High for containerized deployments (crashes on `set_logging_config()`)
**Affected:** Any ECS/EKS/Lambda/Fargate deployment with `readOnlyRootFilesystem: true`

---

## Reproduction (Verified)

```python
import os
os.chdir('/')  # simulate read-only container CWD

from graphrag_toolkit.lexical_graph import set_logging_config
set_logging_config('INFO')
# ValueError: Unable to configure handler 'file_handler'
# Caused by: OSError: [Errno 30] Read-only file system: '/output.log'
```

Every example notebook calls `set_logging_config('INFO')`. Every production deployment will call it. On read-only FS, the application crashes at startup.

---

## Root Cause (Deep)

`BASE_LOGGING_CONFIG` in `logging.py` defines a `file_handler`:

```python
'handlers': {
    'stdout': { ... },
    'file_handler': {
        'class': 'logging.FileHandler',
        'filename': 'output.log',      # ← relative to CWD
        'mode': 'a',
    }
},
'loggers': {'': {'handlers': ['stdout'], 'level': logging.INFO}},
#                  ^^^^^^^^ only stdout is active
```

The `file_handler` is **not assigned to any logger**. It appears dormant. But:

**Python's `logging.config.dictConfig()` instantiates ALL handlers defined in the config dict, regardless of whether they're assigned to a logger.** This is by design — handlers can be referenced later or shared across loggers.

When `dictConfig` tries to create `logging.FileHandler('output.log')`:
1. `FileHandler.__init__` calls `open('output.log', 'a')`
2. On read-only FS → `OSError: Read-only file system`
3. `dictConfig` wraps it → `ValueError: Unable to configure handler 'file_handler'`
4. **Entire logging configuration fails**
5. Application crashes

---

## Why `LOG_OUTPUT_DIR` Doesn't Help

The code has this in `config.py`:
```python
DEFAULT_LOG_OUTPUT_DIR = None  # Log file directory (None = use filename as-is, set to /tmp for EKS)
```

And in `set_advanced_logging_config()`:
```python
if filename:
    if GraphRAGConfig.log_output_dir and not os.path.isabs(filename):
        filename = os.path.join(GraphRAGConfig.log_output_dir, filename)
```

**But this only applies when the user passes `filename` explicitly.** The `BASE_LOGGING_CONFIG` already has `'filename': 'output.log'` hardcoded — `LOG_OUTPUT_DIR` doesn't modify the base config. The `dictConfig` call processes the hardcoded path regardless.

Setting `LOG_OUTPUT_DIR=/tmp` does NOT fix the crash. Verified:
```
Workaround 3: Set LOG_OUTPUT_DIR=/tmp before calling
  FAILS ✗ — LOG_OUTPUT_DIR doesn't help because BASE_LOGGING_CONFIG is already built
```

---

## Workarounds (Tested)

| Workaround | Works? | Production-viable? |
|------------|--------|-------------------|
| Remove `file_handler` from config before dictConfig | ✓ | No — requires modifying internal constant |
| Add `delay=True` to `file_handler` | ✓ | No — requires modifying internal constant |
| Set `LOG_OUTPUT_DIR=/tmp` | ✗ | — |
| Monkey-patch `BASE_LOGGING_CONFIG` before calling | ✓ | Fragile — breaks on version updates |
| Ensure CWD is writable | ✓ | Violates security best practices |
| Don't call `set_logging_config()` | ✓ | Loses all logging configuration |

**There is no clean user-facing workaround.** The only options are:
1. Don't use `readOnlyRootFilesystem` (security regression)
2. Monkey-patch the library's internal config (fragile)
3. Don't call `set_logging_config()` (lose logging)

---

## Proper Fix Options

### Fix A: Remove `file_handler` from BASE_LOGGING_CONFIG (Best)

Only define it dynamically when `filename` is actually requested:

```python
BASE_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': { ... },
    'formatters': { ... },
    'handlers': {
        'stdout': { ... }
        # file_handler NOT here — added only when needed
    },
    'loggers': {'': {'handlers': ['stdout'], 'level': logging.INFO}},
}

def set_advanced_logging_config(..., filename=None):
    config = copy.deepcopy(BASE_LOGGING_CONFIG)
    ...
    if filename:
        if GraphRAGConfig.log_output_dir and not os.path.isabs(filename):
            filename = os.path.join(GraphRAGConfig.log_output_dir, filename)
        config['handlers']['file_handler'] = {
            'formatter': 'default',
            'class': 'logging.FileHandler',
            'filename': filename,
            'filters': ['moduleFilter'],
            'mode': 'a',
        }
        config['loggers']['']['handlers'].append('file_handler')
    logging.config.dictConfig(config)
```

**Pros:** Clean, minimal change, no behavior change for users, fixes all deployment scenarios.
**Cons:** None.

### Fix B: Add `delay=True` to file_handler (Partial)

```python
'file_handler': {
    'class': 'logging.FileHandler',
    'filename': 'output.log',
    'delay': True,          # ← defer file open until first emit
    'mode': 'a',
}
```

`delay=True` tells `FileHandler` to not open the file until the first log record is emitted to it. Since the handler is never assigned to a logger (unless `filename` is passed), it never opens the file.

**Pros:** 1-line change. dictConfig succeeds.
**Cons:** 
- File still gets opened if user passes `filename` pointing to read-only path
- The handler object exists in memory unnecessarily
- Doesn't solve the conceptual issue (why define a handler you never use?)
- If Python changes `dictConfig` behavior in future, may break again

### Fix C: Conditional handler based on environment (Over-engineered)

Check `os.access(os.getcwd(), os.W_OK)` and only include the handler if writable.

**Cons:** Race condition. CWD can change. Overly clever.

---

## Recommendation

**Fix A is the correct solution.** It's 3 lines moved + 6 lines added. Zero behavior change. Zero risk. The handler should only exist when it's needed.

Fix B is an acceptable quick patch if Fix A is rejected for some reason, but it leaves dead code in the config.

---

## Why This Matters for Production

AWS security best practices for ECS/EKS recommend `readOnlyRootFilesystem: true`:

```yaml
# ECS Task Definition
containerDefinitions:
  - readonlyRootFilesystem: true
    ...

# EKS Pod Security Context
securityContext:
  readOnlyRootFilesystem: true
```

This is:
- Required by many compliance frameworks (CIS Benchmark, SOC 2)
- Default in AWS Fargate platform version 1.4+
- Recommended in AWS Well-Architected Framework (Security Pillar)

The lexical-graph library is **incompatible with AWS's own security recommendations** due to this bug.

---

## Test Plan

```python
import os
os.chdir('/')  # read-only

from graphrag_toolkit.lexical_graph import set_logging_config

# After fix: should succeed
set_logging_config('INFO')
logging.getLogger('test').info('Works on read-only FS')

# File logging still works when explicitly requested with writable path:
set_logging_config('INFO', filename='/tmp/app.log')
assert os.path.exists('/tmp/app.log')
```

---

© 2024-2026 Kanjani AI Research.
