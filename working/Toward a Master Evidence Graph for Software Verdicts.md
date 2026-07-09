# Toward a Master Evidence Graph for Software Verdicts

> PRIVATE AND PROPRIETARY. Owned by Kanjani AI Research. See [NOTICE.md](NOTICE.md).

## 1. Purpose

This paper proposes a method for transforming software evidence artifacts into a governed, stateful, and verdict-supporting master graph.

Modern software organizations possess many evidence artifacts: SBOMs, vulnerability lists, code property graphs, design documents, runtime telemetry, control records, ownership data, and human assessments. These artifacts are usually evaluated in isolation. A vulnerability scanner reports a match. A design document claims an architecture. A code graph shows reachability. A team believes a component is low risk. Each artifact provides evidence, but none alone provides a complete verdict.

The purpose of the master evidence graph is to make these artifacts mathematically comparable, semantically related, and verdict-demanding.

The central claim is:

> Evidence is not the verdict, but evidence demands a verdict.

Evidence becomes organizationally meaningful only when it is related to a concern, a desired state, an evidenced state, a perceived state, and a verdict domain.

## 2. Core Thesis

The proposed model is built on five principles.

First, there must be one common evidence vector. The seven verdict domains must not invent separate evidence universes. They must consume the same variables.

Second, each variable must have state. A variable is not merely a value. It has a desired state, an actual state, an evidenced state, and a perceived state.

Third, relationships are verdict-relative. The same evidence may support a cybersecurity verdict, weaken an architecture verdict, expose an organizational capability gap, or demand a governance review.

Fourth, relationships are time-bound and context-sensitive. At time `t`, under context `c`, concern `A` has relationship `r` to concern `B` with strength `s`.

Fifth, the master graph exists to expose gaps. The most important organizational signals are often not the raw values, but the difference between what was intended, what was evidenced, what was measured, and what was believed.

## 3. The Minimal Evidence Vector

The first version of the model begins with a Software Component Observation Model, or SCOM.

SCOM is not the SBOM. The SBOM is a component declaration artifact. SCOM is a mediated component observation model produced by relating SBOM evidence to vulnerability intelligence, code property graph observations, design claims, and human or document-based assessments.

The minimal SCOM vector contains the following variables:

| Variable                        | Type                       | Meaning                                                    |
| ------------------------------- | -------------------------- | ---------------------------------------------------------- |
| `component_present`             | Binary                     | Component exists in the software                           |
| `component_identity_confidence` | Continuous                 | Confidence that component identity and version are correct |
| `vulnerability_match`           | Binary                     | Component/version matches a known vulnerability            |
| `vulnerability_applicability`   | Continuous                 | Confidence that the vulnerability applies                  |
| `vulnerability_severity`        | Continuous                 | Normalized vulnerability severity                          |
| `static_reachability`           | Continuous                 | CPG evidence suggests reachable use                        |
| `static_usage_confidence`       | Continuous                 | Confidence in the CPG usage signal                         |
| `design_currency`               | Ordinal-derived continuous | Design documentation is current                            |
| `design_alignment`              | Derived continuous         | Design claims align with implementation evidence           |
| `perceived_component_risk`      | Ordinal-derived continuous | Human or organizational perception of component concern    |

These variables form the first common evidence vector.

## 4. Four-State Variable Model

Each variable has four possible states:

| State           | Meaning                                                  |
| --------------- | -------------------------------------------------------- |
| Desired state   | What should be true                                      |
| Actual state    | What is empirically measured                             |
| Evidenced state | What available evidence currently supports               |
| Perceived state | What humans, documents, or organizational claims believe |

A variable is therefore represented as:

`variable = {desired_state, actual_state, evidenced_state, perceived_state}`

Actual state may only be assigned when empirically measured. Evidenced state is the best currently supportable value from available artifacts. Perceived state represents claims, assumptions, Likert assessments, design beliefs, or team understanding.

The graph must calculate gaps between these states:

| Gap             | Formula                                  | Meaning                                         |
| --------------- | ---------------------------------------- | ----------------------------------------------- |
| Target gap      | `abs(desired_state - evidenced_state)`   | Difference between required and evidenced truth |
| Perception gap  | `abs(perceived_state - evidenced_state)` | Difference between belief and evidence          |
| Measurement gap | `abs(actual_state - evidenced_state)`    | Difference between measured and evidenced truth |
| Governance gap  | `abs(desired_state - perceived_state)`   | Difference between intent and belief            |

These gaps are the primary mechanisms by which evidence becomes verdict-demanding.

## 5. Seven Verdict Domains

The model defines seven verdict domains:

1. Software Engineering
2. Cybersecurity
3. Architecture and Dependency
4. Operational Capacity
5. Organizational Capability
6. Governance and Legitimacy
7. Business and Mission Impact

Each verdict domain consumes the same evidence vector, but applies different relationships and weights.

The general form is:

`VerdictConcern_d(t,c) = W_d · C(t,c)`

Where:

* `d` is the verdict domain.
* `W_d` is the verdict-specific weight vector.
* `C(t,c)` is the transformed common concern vector at time `t` under context `c`.

The variables remain the same. The verdict interpretation changes because the relationship and weighting change.

## 6. Master Graph Objective

The master graph is not merely an evidence store. It is a mediated relationship graph that connects artifacts, variables, states, concerns, gaps, verdict demands, and verdicts.

The graph must answer questions such as:

* What evidence exists?
* What variable does the evidence support?
* Which state does the evidence populate?
* What gap does the evidence expose?
* Which verdict domain does the gap demand?
* What additional evidence is missing?
* How strong is the concern?
* How does the concern change over time and context?
* Which verdicts are currently underdetermined?

The master graph therefore acts as the structure through which software evidence becomes organizational intelligence.

## 7. Initial Master Graph Pattern

The minimal graph pattern is:

`Evidence Artifact → Evidence Element → SCOM → Evidence Variable → Variable State → State Gap → Verdict Demand → Verdict Domain → Verdict`

The first implementation begins with:

* SBOM evidence
* Vulnerability-list evidence
* CPG evidence
* Design and Likert assessment evidence

These artifacts create the first SCOM vector. The SCOM vector supports the seven verdict domains. The gaps reveal what is known, unknown, misaligned, contradicted, or merely perceived.

## 8. Working Definition

A master evidence graph is a time-bound, context-sensitive graph that represents how evidence artifacts produce quantitative variables, how those variables express desired, actual, evidenced, and perceived states, how state gaps demand verdicts, and how seven verdict domains project meaning from the same common evidence vector.
