# Defining the Master Evidence Graph: A Formal Model for Stateful Software Verdicts

> PRIVATE AND PROPRIETARY. Owned by Kanjani AI Research. See [NOTICE.md](NOTICE.md).

## 1. Purpose

The first paper introduced the need for a master evidence graph: a governed structure that converts software artifacts into stateful, verdict-demanding evidence. This paper defines the formal structure of that graph.

The objective is to define a graph model that can represent:

1. Evidence artifacts
2. Evidence elements
3. Composite observation models
4. Common vector variables
5. Variable states
6. State gaps
7. Verdict demands
8. Seven verdict-domain projections
9. Time-bound and context-sensitive relationships

The central design rule is:

> The master graph must use one common evidence vector. The seven verdicts do not use different evidence universes. They use the same variables with different relationships, weights, and standards of judgment.

## 2. Core Graph Principle

The master evidence graph is not simply a storage graph. It is a mediated graph of meaning.

The graph does not ask only:

> What evidence exists?

It asks:

> What does this evidence support, which state does it affect, what gap does it expose, and which verdict does it demand?

The minimal chain is:

`Evidence Artifact → Evidence Element → Composite Observation Model → Evidence Variable → Variable State → State Gap → Verdict Demand → Verdict Domain → Verdict`

This chain separates raw artifact existence from organizational judgment.

## 3. Node Types

The master graph contains the following minimum node classes.

| Node Type                   | Purpose                                                                        |
| --------------------------- | ------------------------------------------------------------------------------ |
| `EvidenceArtifact`          | Raw artifact such as SBOM, CVE list, CPG, design document, assessment          |
| `EvidenceElement`           | Extracted fact or observation from an artifact                                 |
| `CompositeObservationModel` | Mediated object combining related evidence elements                            |
| `EvidenceVariable`          | Named quantitative or categorical variable derived from evidence               |
| `VariableState`             | Desired, actual, evidenced, or perceived state of a variable                   |
| `StateGap`                  | Difference between two states                                                  |
| `VerdictDemand`             | Trigger that requires a verdict question                                       |
| `VerdictDomain`             | One of the seven domains                                                       |
| `VerdictProjection`         | Domain-specific weighting over the common vector                               |
| `Verdict`                   | Resulting mediated judgment                                                    |
| `ConcernRelationship`       | Relationship between concerns at time `t` and context `c`                      |
| `Context`                   | Scope, system, environment, time, tenant, deployment, or operational condition |
| `Authority`                 | Policy, owner, reviewer, or governance authority                               |
| `EvidenceSource`            | Tool, observer, system, or person that produced evidence                       |

## 4. Edge Types

The minimum graph requires the following relationship types.

| Edge Type                | Meaning                                                           |
| ------------------------ | ----------------------------------------------------------------- |
| `PRODUCES`               | Artifact holder produces an evidence artifact or element          |
| `EXTRACTS`               | Observer extracts evidence from an artifact                       |
| `DECLARES_COMPONENT`     | SBOM declares a software component                                |
| `DECLARES_VULNERABILITY` | Vulnerability source declares vulnerability fact                  |
| `MATCHES_AFFECTED_RANGE` | Component version matches vulnerability affected range            |
| `EVIDENCES_STATIC_USE`   | CPG evidence shows static component or function usage             |
| `EVIDENCES_REACHABILITY` | CPG evidence supports reachability                                |
| `CLAIMS_DESIGN_STATE`    | Design document claims intended state                             |
| `SCORES_PERCEPTION`      | Likert or assessment score produces perceived state               |
| `DERIVES_VARIABLE`       | Observation model derives an evidence variable                    |
| `POPULATES_STATE`        | Evidence populates desired, actual, evidenced, or perceived state |
| `EXPOSES_GAP`            | State comparison exposes a gap                                    |
| `DEMANDS_VERDICT`        | Gap or variable demands a verdict                                 |
| `SUPPORTS_VERDICT`       | Variable or gap supports verdict-domain calculation               |
| `AMPLIFIES`              | One variable increases concern in another                         |
| `WEAKENS`                | One variable reduces concern or confidence                        |
| `CORROBORATES`           | Evidence independently supports another observation               |
| `CONTRADICTS`            | Evidence conflicts with another observation                       |
| `RELATES_CONCERN`        | Concern-to-concern relationship with type and strength            |
| `HAS_CONTEXT`            | Node or edge applies within context                               |
| `HAS_AUTHORITY`          | Verdict or state is linked to authority                           |

## 5. Composite Observation Model: SCOM

The first composite model is the Software Component Observation Model, or SCOM.

SCOM is produced by relating:

* SBOM component evidence
* Vulnerability-list evidence
* CPG reachability evidence
* Design-document evidence
* Likert or assessment evidence

SCOM is not the SBOM. SBOM declares component facts. SCOM relates component facts to vulnerability, code, design, and perception evidence.

Formal expression:

`SCOM = f(SBOM, VulnerabilityList, CPG, DesignEvidence, AssessmentEvidence)`

SCOM then derives the minimal evidence vector.

## 6. Minimal Common Evidence Vector

The initial SCOM vector contains the following variables.

| Variable                        | Type                       | Meaning                                                   |
| ------------------------------- | -------------------------- | --------------------------------------------------------- |
| `component_present`             | Binary                     | Component exists in software                              |
| `component_identity_confidence` | Continuous                 | Confidence that component identity/version is correct     |
| `vulnerability_match`           | Binary                     | Component/version matches known vulnerability             |
| `vulnerability_applicability`   | Continuous                 | Confidence that vulnerability applies                     |
| `vulnerability_severity`        | Continuous                 | Normalized severity                                       |
| `static_reachability`           | Continuous                 | CPG suggests vulnerable or relevant function is reachable |
| `static_usage_confidence`       | Continuous                 | Confidence in CPG usage signal                            |
| `design_currency`               | Ordinal-derived continuous | Design document is current                                |
| `design_alignment`              | Derived continuous         | Design aligns with observed implementation                |
| `perceived_component_risk`      | Ordinal-derived continuous | Human or organizational perception of concern             |

The common vector is:

`X = [component_present, component_identity_confidence, vulnerability_match, vulnerability_applicability, vulnerability_severity, static_reachability, static_usage_confidence, design_currency, design_alignment, perceived_component_risk]`

Every verdict domain uses this same vector.

## 7. Variable State Model

Each variable has four states.

| State           | Meaning                                                  |
| --------------- | -------------------------------------------------------- |
| Desired state   | What should be true                                      |
| Actual state    | What is empirically measured                             |
| Evidenced state | What available evidence currently supports               |
| Perceived state | What humans, documents, or organizational claims believe |

Formal form:

`variable_i = {desired_i, actual_i, evidenced_i, perceived_i}`

Actual state must only be assigned when empirically measured. Evidenced state is the best currently supportable value from available artifacts. Perceived state comes from claims, documents, Likert assessments, or organizational belief.

## 8. State Gap Functions

The graph must calculate gaps between states.

| Gap               | Formula                                                   | Meaning                                               |
| ----------------- | --------------------------------------------------------- | ----------------------------------------------------- |
| `target_gap`      | `abs(desired - evidenced)`                                | Difference between required state and evidenced state |
| `perception_gap`  | `abs(perceived - evidenced)`                              | Difference between belief and evidence                |
| `measurement_gap` | `abs(actual - evidenced)`                                 | Difference between measured and evidenced state       |
| `governance_gap`  | `abs(desired - perceived)`                                | Difference between intent and belief                  |
| `design_gap`      | `1 - design_alignment`                                    | Difference between design and implementation          |
| `awareness_gap`   | `abs(perceived_component_risk - component_concern_score)` | Difference between perceived and evidenced concern    |

These gaps are the primary mechanisms by which evidence becomes verdict-demanding.

## 9. Concern Vector

The common evidence vector contains both concern-increasing and concern-reducing variables. Therefore, the graph computes a transformed concern vector.

Concern-increasing examples:

* `vulnerability_match`
* `vulnerability_applicability`
* `vulnerability_severity`
* `static_reachability`
* `perceived_component_risk`

Concern-reducing examples:

* `component_identity_confidence`
* `static_usage_confidence`
* `design_currency`
* `design_alignment`

The transformed concern vector is:

`C = [component_present, identity_uncertainty, vulnerability_match, vulnerability_applicability, vulnerability_severity, static_reachability, usage_uncertainty, design_currency_concern, design_alignment_concern, perceived_component_risk]`

Where:

`identity_uncertainty = 1 - component_identity_confidence`

`usage_uncertainty = 1 - static_usage_confidence`

`design_currency_concern = 1 - design_currency`

`design_alignment_concern = 1 - design_alignment`

## 10. Seven Verdict Domains

The graph supports seven verdict domains.

| Verdict Domain              | Central Question                                                           |
| --------------------------- | -------------------------------------------------------------------------- |
| Software Engineering        | Does the implementation create engineering concern?                        |
| Cybersecurity               | Does the software contain a meaningful security concern?                   |
| Architecture and Dependency | Does implementation match intended architecture and dependency design?     |
| Operational Capacity        | Does the component or code path create operational capacity concern?       |
| Organizational Capability   | Can the organization understand, sustain, and remediate the concern?       |
| Governance and Legitimacy   | Is the state aligned with policy, authority, evidence, and accountability? |
| Business and Mission Impact | Does the concern matter materially to business or mission outcomes?        |

Each verdict is a projection over the same concern vector.

General form:

`Concern_d(t,c) = W_d · C(t,c)`

Where:

* `d` is the verdict domain
* `W_d` is the domain-specific weight vector
* `C(t,c)` is the concern vector at time `t` and context `c`

## 11. Verdict-Domain Projections

The seven verdict projections are:

`Concern_SE = W_SE · C`

`Concern_SEC = W_SEC · C`

`Concern_ARCH = W_ARCH · C`

`Concern_CAP = W_CAP · C`

`Concern_ORG = W_ORG · C`

`Concern_GOV = W_GOV · C`

`Concern_BUS = W_BUS · C`

The variables remain the same. The weight vector changes.

This allows one master graph to produce a spider or radar profile of software concern across seven verdict domains.

## 12. Time-Bound Concern Relationships

The master graph must treat relationships as time-bound and context-sensitive.

The fundamental relationship is:

> At time `t`, under context `c`, concern `A` has relationship `r` to concern `B` with strength `s`.

Formal form:

`R(A, B, t, c) = (r, s)`

Where:

* `A` is the source concern
* `B` is the target concern
* `t` is time or validity window
* `c` is context
* `r` is relationship type
* `s` is relationship strength

Relationship type:

`r ∈ {complementary, opposing, neutral, conditional}`

Strength:

`s ∈ [-1, +1]`

This permits relationships such as:

* Security may oppose performance in one context.
* Security may complement governance in another context.
* Observability may complement reliability.
* Design currency may weaken over time.
* Vulnerability severity may become more material when exploit activity increases.

Time is not metadata. Time is an operand in the verdict function.

## 13. Verdict Demand

Evidence does not equal verdict. Evidence demands a verdict when it exposes a meaningful gap or crosses a concern threshold.

Formal form:

`VerdictDemand = demand(variable, state_gap, threshold, context, domain)`

A verdict demand is triggered when:

* Desired state conflicts with evidenced state
* Perceived state conflicts with evidenced state
* Actual state conflicts with evidenced state
* Evidence exceeds threshold
* Evidence contradicts design or policy
* Evidence is insufficient for a required verdict

Example:

If:

`vulnerability_match_desired = 0`

and:

`vulnerability_match_evidenced = 1`

then:

`target_gap = 1`

This demands at least a cybersecurity verdict and likely a governance verdict.

## 14. Master Graph Definition

The master evidence graph can now be defined as:

`G_master = (N, E, X, S, Δ, R, W, V)`

Where:

* `N` is the set of graph nodes
* `E` is the set of graph edges
* `X` is the common evidence vector
* `S` is the variable state model
* `Δ` is the set of state-gap functions
* `R` is the set of time-bound concern relationships
* `W` is the verdict-domain weight matrix
* `V` is the set of verdict outputs

The master graph is therefore not one graph of facts. It is a graph of evidence, state, relationship, gap, concern, and verdict.

## 15. First Implementation Boundary

The first implementation should be limited to SCOM.

Inputs:

* SBOM
* Vulnerability list
* CPG
* Design document
* Likert assessment

Outputs:

* SCOM
* Minimal evidence vector
* Four-state variable model
* State gaps
* Seven preliminary verdict concern scores
* Missing-evidence map

The purpose of the first implementation is not to produce final enterprise verdicts. The purpose is to prove that a common evidence vector can support multiple verdict projections and reveal where evidence is missing.

## 16. Working Conclusion

The master evidence graph provides a disciplined way to move from isolated software artifacts to mediated organizational judgment. It does this by preserving one common evidence vector, assigning each variable state, calculating gaps, applying verdict-domain relationships, and producing concern scores across seven verdict domains.

The result is not merely a graph of software. It is a graph of what the organization knows, believes, intends, measures, lacks, and must decide.
