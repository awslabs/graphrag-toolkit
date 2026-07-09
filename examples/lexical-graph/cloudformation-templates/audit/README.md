# GraphRAG Toolkit — Deployment Audit

A comprehensive audit script for discovering and inspecting AWS resources deployed by the GraphRAG Toolkit CloudFormation templates — or deployed manually without CloudFormation.

## Quick Start

```bash
# Stack-based audit (auto-detects graphrag CloudFormation stacks)
./audit-deployment.sh --profile <your-profile> --region us-east-1

# Manual audit (scans by resource type, no stack required)
./audit-deployment.sh --manual --profile <your-profile> --region us-east-1
```

## Files

| File | Description |
|------|-------------|
| `audit-deployment.sh` | Main audit script (bash) |
| `audit-resources.json` | Resource type configuration for manual scan mode |
| `README.md` | This file |

## Modes

### Stack Mode (default)

Audits resources managed by a CloudFormation stack. The script will:
1. Auto-detect stacks with "graphrag" in the name, or use `--stack-name`
2. List all stack resources and check their status
3. Run drift detection
4. Inspect each resource type for detailed health info

```bash
./audit-deployment.sh --stack-name my-graphrag-stack --region us-west-2
```

### Manual Mode (`--manual`)

Scans the AWS account directly by resource type — no CloudFormation stack required. Useful when resources were created manually, via CLI, or through other IaC tools.

```bash
./audit-deployment.sh --manual --profile production --region us-east-1
```

The scan is driven by `audit-resources.json` which defines what to look for.

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--stack-name NAME` | CloudFormation stack name | Auto-detect |
| `--region REGION` | AWS region | `us-east-1` |
| `--profile PROFILE` | AWS CLI profile name | Default profile |
| `--manual` | Scan by resource type (no stack required) | Off |
| `--resources FILE` | Custom resource config JSON (implies `--manual`) | `audit-resources.json` |
| `--json` | Machine-readable JSON output | Off |
| `--help` | Show usage | — |

## Configuring Manual Scan (`audit-resources.json`)

The config file controls which AWS resource types the manual scan discovers. Each entry has:

```json
{
  "type": "neptune_clusters",
  "label": "Neptune DB Clusters",
  "enabled": true,
  "service": "neptune",
  "operation": "describe-db-clusters",
  "jq_extract": ".DBClusters[].DBClusterIdentifier",
  "logical_id": "NeptuneDBCluster",
  "cfn_type": "AWS::Neptune::DBCluster"
}
```

| Field | Purpose |
|-------|---------|
| `enabled` | `true` to scan, `false` to skip |
| `service` | AWS CLI service name |
| `operation` | AWS CLI operation (with any flags like `--type encryption`) |
| `jq_extract` | jq expression to extract resource identifiers from the response |
| `logical_id` | Logical name used internally for audit mapping |
| `cfn_type` | CloudFormation resource type label |

### Default Configuration

**Enabled** (scanned by default):
- Neptune DB Clusters & Instances
- Neptune Analytics Graphs
- OpenSearch Serverless Collections
- OpenSearch Encryption, Network, and Access Policies
- SageMaker Notebook Instances
- S3 Buckets

**Disabled** (available but off by default):
- Aurora PostgreSQL Clusters & Instances
- Lambda Functions
- IAM Roles & Customer Managed Policies
- VPCs (with subnets, security groups, NAT/IGW, endpoints)

To enable a resource type, set `"enabled": true` in the JSON file.

### Custom Config

```bash
# Use a custom resource list
./audit-deployment.sh --resources my-scan-config.json --profile nw
```

## Resource Types Audited

The script provides detailed inspection for these resource categories:

| Category | Details Shown |
|----------|---------------|
| **Neptune DB** | Cluster status, endpoint, engine version, serverless NCU config, instances |
| **Neptune Analytics** | Graph ID, memory (GiB), vector search, endpoint |
| **OpenSearch Serverless** | Collection status, type, endpoints, all policies |
| **Aurora PostgreSQL** | Cluster status, serverless v2 config, instances |
| **S3 Buckets** | Bucket names, KMS keys |
| **SageMaker** | Notebook status, instance type, volume, running cost |
| **IAM** | Roles and customer-managed policies |
| **Lambda** | Function names, runtimes, memory |
| **VPC/Networking** | VPCs, subnets, security groups, NAT/IGW, VPC endpoints |
| **CloudFormation** | Stack status, outputs, parameters, drift detection |

## Output

### Terminal (default)

Colorized output with:
- Section headers
- Status icons: ✓ (healthy), ⚠ (warning), ✗ (error), ℹ (info)
- Cost indicators for billable resources
- Health score bar

### JSON (`--json`)

```json
{
  "audit_metadata": { "timestamp": "...", "region": "...", "version": "1.1.0" },
  "summary": {
    "total_stack_resources": 95,
    "resources_verified": 95,
    "resources_healthy": 27,
    "health_percentage": 28
  },
  "stack_resources": [ ... ],
  "stack_info": { ... }
}
```

## Prerequisites

- **AWS CLI v2** — configured with valid credentials
- **jq** — JSON processor (`brew install jq` / `apt install jq`)
- **bash 3.2+** — compatible with macOS default bash

## Examples

```bash
# Audit everything enabled in the config
./audit-deployment.sh --manual --profile nw

# Just Neptune and OpenSearch (create a minimal config)
./audit-deployment.sh --resources neptune-only.json --profile nw

# Stack-based with JSON output for CI/CD
./audit-deployment.sh --stack-name graphrag-prod --json > audit-report.json

# Pipe JSON to jq for specific info
./audit-deployment.sh --manual --json | jq '.stack_resources | group_by(.ResourceType) | map({type: .[0].ResourceType, count: length})'
```

## Cost Awareness

The audit highlights running resources that incur charges:

| Resource | Cost Note |
|----------|-----------|
| Neptune Serverless | ~$0.1028/NCU-hour |
| OpenSearch Serverless | ~$0.24/OCU-hour (min 4 OCUs) |
| SageMaker Notebooks | Varies by instance type (only when running) |
| NAT Gateways | ~$0.045/hour + data processing |
| Neptune DB (provisioned) | Varies by instance class |

## License

Internal tool — part of the GraphRAG Toolkit examples.
