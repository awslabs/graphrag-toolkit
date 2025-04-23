# Using AWS Profiles in `GraphRAGConfig`

This guide explains how to configure and use **AWS named profiles** within the GraphRAG Toolkit by leveraging the `GraphRAGConfig` class.

## 🔍 What is an AWS Profile?

AWS CLI and SDKs allow the use of named profiles to manage different sets of credentials. Each profile typically contains:
- Access key ID
- Secret access key
- (Optional) Session token
- (Optional) Default region

These profiles are stored in:
- `~/.aws/credentials`
- `~/.aws/config`

---

## How `GraphRAGConfig` Uses AWS Profiles

### 1. **Automatic Detection**
If no profile is explicitly provided, `GraphRAGConfig` attempts to use:
```python
os.environ.get("AWS_PROFILE")
```

If that’s not set, it will fall back to the default AWS behavior.

---

### 2. **Explicit Profile Setting**

You can programmatically set a profile:

```python
from graphrag_toolkit.config import GraphRAGConfig

GraphRAGConfig.aws_profile = "padmin"
```

This automatically resets any previously cached clients or sessions to ensure all AWS service interactions use the new credentials.

---

### 3. **Where Profiles are Used**

When you call:

```python
GraphRAGConfig.session
```

or use properties like:

```python
GraphRAGConfig.bedrock
GraphRAGConfig.s3
GraphRAGConfig.rds
```

the SDK creates the respective clients using the active profile and region.

---

## Example with Environment Variables

You can export the profile and region before running your app:

```bash
export AWS_PROFILE=padmin
export AWS_REGION=us-east-1
python my_app.py
```

Or set them inline:

```bash
AWS_PROFILE=padmin AWS_REGION=us-east-1 python my_app.py
```

---

## Profile-Based Multi-Account Testing

To test across AWS accounts:
```python
GraphRAGConfig.aws_profile = "dev-profile"
GraphRAGConfig.aws_region = "us-west-2"

bedrock = GraphRAGConfig.bedrock  # Will use dev-profile in us-west-2
```

---

## ❗Common Pitfalls

- **Missing Profile**: Ensure the profile exists in `~/.aws/credentials` and is not misspelled.
- **Access Denied**: Check IAM permissions for the services you're trying to access.
- **Region mismatch**: Bedrock may only be available in specific regions (e.g., `us-east-1`).

---

## Summary

| Use Case                     | How to Do It                                              |
|-----------------------------|------------------------------------------------------------|
| Default profile              | Rely on environment variables or default config           |
| Programmatic override        | `GraphRAGConfig.aws_profile = "my-profile"`               |
| Switch regions               | `GraphRAGConfig.aws_region = "us-east-2"`                 |
| Full override                | Set both profile and region before invoking `.session`    |
| Create boto3 clients         | Use `.bedrock`, `.s3`, or `.rds` properties               |