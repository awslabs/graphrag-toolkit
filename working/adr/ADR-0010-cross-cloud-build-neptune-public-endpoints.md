# ADR-0010: Cross-Cloud Build Topology and Neptune Public Endpoints

- Status: Proposed - client decision required
- Date: 2026-06-29
- Stance: We propose and inform; the client decides (ADR-0001 section 1).
- Refines: ADR-0003 (store topology), ADR-0006 (external extraction/build), ADR-0008 (cloud strategy)

## 1. Context
A client scenario: extraction runs on Azure EKS during an active Azure -> AWS migration; the client wants agents/build in Azure talking to AWS Neptune + OpenSearch. Two facts govern this:
1. Amazon Neptune now supports public endpoints (engine >= 1.4.6.x): per-instance publicly-accessible, IAM DB auth mandatory, security-group controlled, instances in public subnets with an internet-gateway route. Previously Neptune was strictly VPC-private. AWS positions public endpoints as a dev/test convenience.
2. The toolkit connects to Neptune Database via the neptunedata boto3 client over HTTPS with SigV4 (verified) - the same path a public endpoint serves. OpenSearch Serverless supports public endpoints with SigV4 + data-access policies as a standard configuration.

Therefore the Azure -> AWS build is technically feasible with no toolkit code change.

## 2. Decision
1. The toolkit requires no change to write to Neptune/OpenSearch over public endpoints - point the connection at the public endpoint URL and supply AWS credentials + region for SigV4.
2. Public-endpoint cross-cloud build is APPROVED for the interim/dev/test/POC period only.
3. For the end-state (production), keep Neptune private and use either:
   (a) extract in Azure -> stage artifact in S3 -> build in AWS in-VPC (ADR-0006), or
   (b) private connectivity (VPN / Direct Connect + ExpressRoute / Transit Gateway).
4. Any Neptune-touching component (build AND query/retrieval) inherits this; only extract may run cross-cloud freely.

## 3. Requirements for the public-endpoint path (interim)
- Neptune engine >= 1.4.6.x; publicly-accessible on ALL instances (failover role-swap - see risks).
- IAM DB auth enabled.
- Security-group allowlist for the Azure EKS egress CIDR (stable NAT egress), not 0.0.0.0/0.
- Neptune instances in public subnets with an internet-gateway route.
- Azure EKS -> AWS IAM credentials via workload-identity federation (OIDC) preferred over static keys.
- OpenSearch Serverless public network policy + SigV4 data-access policy (IAM-principal-gated).

## 4. Risks / caveats
- AWS positions public endpoints as dev/test convenience, not production.
- Public subnets for a proprietary CPG/evidence graph is a security-posture regression.
- Failover role-swap: public is per-instance; the cluster endpoint is public only if the writer is public; on failover a public-writer/private-reader flips and the build loses write access - HA-safe requires ALL instances public (maximum exposure).
- Write-heavy build over public internet: WAN latency per batch, throughput limits, transient errors, egress/data-transfer cost; painful at hundreds-of-projects scale.
- Org governance: an IAM/SCP guardrail may deny public instance creation.

## 5. Component placement (interim vs end-state)
| Stage | Touches Neptune? | Interim | End-state |
|---|---|---|---|
| Joern extract -> JSON | No | Azure | Azure |
| Per-node enrich | No | Azure | Azure or AWS |
| Build (writes graph + vectors) | Yes | Azure->public endpoint OR AWS | AWS in-VPC |
| Query / retrieval / agents | Yes | Azure->public endpoint OR AWS | AWS in-VPC |
| Traversal-based enrich | Yes | AWS | AWS |

## 6. Consequences
- POC/interim unblocked quickly with no toolkit change.
- The extract -> S3 -> in-AWS-build split (ADR-0006) remains the durable pattern; even with public endpoints the write-heavy build favors running in AWS.
- Model hosting cascades (ADR-0007): if build runs in AWS, host the embedding model in AWS; the generation model can remain in Azure with enrichment.

## 7. Open questions for the client
1. Is public-endpoint use acceptable to the client's AWS governance (SCP)?
2. Interim only, or is a production cross-cloud build being contemplated?
3. HA requirement for Neptune during the interim (drives all-instances-public exposure)?
4. Expected build volume (WAN write cost at scale)?
5. Target date for the sunset to private endpoints.
