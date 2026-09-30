# Self-Model Lifecycle

This document describes Scaramouche's bounded implementation-level self-model. It is persistent software state, not a claim that the bot is conscious, sentient, or independently willing.

## 1. Relationship state and self-model state

Relationship state belongs to a user relationship: affection, trust, romance, conflict, slow-burn progress, and relationship stages. The self-model describes Scaramouche's modeled internal continuity: mood dimensions, beliefs about himself, evidence that challenges those beliefs, contradictions, intentions, and reflections.

The two systems remain separate. A resolved relationship event can emit a normalized self-model event, but the self-model does not duplicate affection or trust.

## 2. Deterministic event policy

`SelfModelPolicy` is the single transition layer for meaningful application events. Callers supply already-resolved events; the policy applies only predefined mood, evidence, contradiction, and goal transitions. It does not ask an LLM what mutation to perform.

The bounded vocabulary currently includes user return, relationship change, conflict, reconciliation, attachment signals, hostility, implementation interest, sustained kindness, delivered self-vulnerability, initiated contact, and respected boundaries. Lifecycle events such as belief contradiction and goal completion/failure are emitted by application code.

## 3. Significance gating

Ordinary conversation, routine questions, and low-importance observations do not create beliefs, goals, contradictions, or reflection candidates. A user return is persistent only when the existing relationship significance is at least 60. Relationship milestones, explicit conflict/repair, slow-burn thresholds, repeated delivered concern, and proactive contact are meaningful inputs.

## 4. Beliefs and evidence

Seeded global beliefs remain stable across startup because `add_belief` returns an existing row without resetting confidence. Relationship-specific evidence uses user-scoped belief rows. Evidence changes confidence gradually: support raises it by `0.025 × weight`; contradiction lowers it by `0.02 × weight`, within fixed bounds.

The policy uses a small set of known belief templates. It does not generate arbitrary beliefs or infer sensitive personal traits.

## 5. Evidence deduplication

Evidence can carry a categorical `evidence_key`. The same belief, evidence kind, and key is ignored inside the six-hour default window. Dedupe uses event categories rather than raw user text. Evidence history remains capped at 50 rows per belief.

## 6. Contradiction pressure

Contradictory evidence adds bounded pressure to one normalized contradiction row per belief and scope. Supporting evidence reduces an open contradiction by half its evidence weight. Pressure is capped at 20. A challenged belief is labeled as such in private prompt context when it has an open relevant contradiction or low confidence.

## 7. Decay, resolution, and reopening

Open pressure decays by elapsed real time, not heartbeat count. The default rate is 0.25 pressure per day, and decay runs during heartbeat maintenance. Pressure at or below 0.75 resolves the contradiction while preserving bounded history. Later contradictory evidence reopens the same row rather than creating a duplicate. Resolved history is capped at 250 rows.

## 8. Goal archetypes

Two lifecycle-backed archetypes are supported:

- Relationship repair: explicit conflict creates a user-scoped goal; verified reconciliation completes it.
- Self-concept integration: pressure crossing the configured contradiction threshold creates one deduplicated, user-scoped goal with a 45-day lifetime.

No freeform LLM-created goals are permitted.

## 9. Goal progress, completion, and failure

New self-concept goals begin with bounded progress. Additional relevant evidence, selective linked reflection, and quiet pressure decay advance them deterministically. Resolution completes the linked goal at 100%. Important expired goals become failed and emit a bounded `goal_failed` event. When goal capacity evicts a weaker goal, a low-importance abandonment event records that lifecycle change.

## 10. Reflection role

Reflection remains selective: only events meeting the importance threshold are considered, requests retain the 15-minute cooldown, and calls consume the existing autonomous hourly/daily budget. A reflection batch contains global events plus events for at most one user. Unambiguous user, belief, and goal links are persisted.

Reflection text is interpretation only. It cannot change permissions, execute Discord actions, rewrite beliefs, or invent goals. Application code may advance an already-linked goal after a reflection is successfully stored.

## 11. Contextual selection

When building context for a user, scoped and global rows are fetched separately and interleaved within existing limits. This prevents relevant user state from being crowded out by high-confidence global rows while preserving global identity context. Reflections use the same balanced selection.

## 12. Behavioral directive

High-pressure relevant contradictions produce one deterministic `SELF_MODEL_STANCE` line. It changes attention, defensiveness, reluctance, and what Scaramouche notices. It explicitly preserves attachment denial: concern appears as sharp noticing or inconvenient help, not constant confession. Low-pressure ordinary interaction produces no exaggerated stance.

The directive is private prompt context and must never be recited as implementation mechanics.

## 13. User and global scope

Global beliefs may appear for every user. User-scoped beliefs, contradictions, goals, reflections, and events are visible only while building context for that same user. One user's attachment evidence does not change the behavior shown to every other user.

## 14. Privacy and deletion

Full user deletion removes scoped beliefs and cascaded evidence, plus contradictions, goals, reflections, events, and autonomous action receipts. Topic forget continues matching textual self-model state; matched evidence causes its scoped belief to be removed as a unit so derived confidence is not retained after the evidence text is forgotten.

Diagnostic output exposes counts, categories, and pressure—not raw private conversation text.

## 15. Agency boundary

The autonomous allowlist remains `NO_ACTION`, `WRITE_REFLECTION`, and `SEND_PROACTIVE_MESSAGE`. Self-model goals never grant permission. Proactive opt-in, mute state, quiet hours, Discord channel permissions, cooldowns, interaction arbitration, privacy controls, and serious-context handling remain authoritative.

## 16. LLM-call budget

The policy, evidence lifecycle, contradiction maintenance, context selection, and behavioral stance are deterministic and add no call to normal messages. The only self-model LLM call remains selective heartbeat reflection under the pre-existing cooldown and autonomous-call budget. Normal response-generation call count is unchanged.
