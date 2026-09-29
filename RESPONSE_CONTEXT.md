# Response Context Architecture

`get_response()` is the coordinator for response generation. Discord routing and
delivery remain in the staged message pipeline documented in
`MESSAGE_PIPELINE.md`.

## Raw state and resolved state

`RawRelationshipState` contains the persisted per-user inputs: mood, affection,
trust, drift, summaries, conflict, callbacks, and repair count. These values are
evidence, not competing instructions to the model.

The existing deterministic `resolve_character()` policy converts the raw user
state, interaction arbitration, and compact self-model dimensions into one
`ResolvedCharacterState`. Its precedence remains:

1. user boundary, consent, and permissions;
2. protective or serious accuracy;
3. structured-session ownership;
4. unresolved conflict;
5. materially relevant concern;
6. Scaramouche's normal proud, theatrical register.

Relationship warmth is derived only from the current user's affection and
trust. A high global attachment dimension may influence cadence through the
self-model, but cannot grant a different user intimate treatment.

## ResponseContext

`ResponseContext` represents one response and groups:

- `ResponseRequest`: immutable request identity and channel options;
- `RawRelationshipState`: persisted per-user inputs;
- `DerivedResponseState`: time, length, arc, progression, repeat count, and
  one-time learning signals;
- `ResolvedCharacterState`: authoritative behavioral interpretation;
- bounded history, recent replies, compact self-model context, enrichments,
  and ordered `PromptFragments`.

Scenario detection, emotional triggers, sentiment, memory events, and scene
updates are calculated once in `derive_response_state()` and reused during
learning. No classifier or stance LLM call is added.

## Prompt order

`PromptFragments` classifies context as identity, raw state, derived state,
behavioral guidance, memory, world, and factual grounding. Their deterministic
order is followed by partner/duo/self/environment/channel enrichments. The
resolved authoritative directive is appended after all lower-priority context,
and the current user message remains the final immediate content.

ARC, PROGRESSION, and EMOTIONAL_LAYER remain separate because they describe
different time scales: relationship phase, accumulated relationship trajectory,
and the present emotional texture. They are categorized together so future
changes can inspect collisions without deleting character nuance.

## Generation and errors

Provider generation is isolated in `_generate_character_reply()`. Only Groq,
network, timeout, or explicitly unconfigured-provider failures increment the
provider-failure monitor. Successful calls increment provider success. SQLite,
context construction, prompt programming, timezone, and post-learning errors
are logged under their own subsystem and never impersonate a provider outage.
`asyncio.CancelledError` always propagates.

Search decision, retrieval, prompt injection, and source appending remain
separate from character-state resolution. Search behavior itself is unchanged.

## Finalization and learning

Generation returns a `GeneratedResponse` containing the draft, its context,
search metadata, attempt count, and fallback status. Then:

1. `_apply_interaction_learning()` applies the unchanged relationship formulas,
   user-message memory events, and scene update;
2. `_claim_response_progression()` reloads post-mutation state and deduplicates
   progression milestones;
3. `_finalize_character_reply()` applies narration stripping, diversification,
   phrase policy, and fallback handling.

User messages are stored once during learning. Assistant conversation history is
stored only after successful Discord delivery. Anti-repeat/self-behavior
bookkeeping is named separately from conversation memory and is deferred by the
normal message pipeline until delivery succeeds.

The optional HOME roommate notification also runs only after a successful DM
delivery. Relay failure is logged under the `home` subsystem and cannot turn a
successful Discord response into a failure.

## Test strategy

Focused tests cover stance precedence and repair, user scope, prompt order,
exact length distributions, malformed timezones, deterministic feature reuse,
provider accounting, unchanged relationship formulas, one-time memory/scene
writes, cancellation at context/search/provider stages, actual-delivery
bookkeeping, HOME behavior, and one-call normal generation.
