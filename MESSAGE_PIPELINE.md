# Message Pipeline

## Authority and stages

`interaction_policy.py` remains the policy authority. `on_message()` is only the
Discord safety boundary; `_dispatch_message()` classifies the event once, assigns
one owner, and stores the shared `InteractionContext` in `CURRENT`. No pipeline
stage performs a second safety classification.

Non-command messages that are not consumed by privacy, boundary, serious-context,
or structured-session handling enter this ordered runtime pipeline:

1. `_prepare_routed_message` resolves runtime data and at most one reply reference.
2. `_observe_prepared_message` performs non-consuming world, self, and count updates.
3. `_handle_pre_response_ownership` handles allowed milestones, anniversaries,
   greetings, and summary persistence.
4. `_handle_media` owns an image/video interaction and prevents normal generation.
5. `_handle_special_trigger` and `_handle_optional_character_behavior` allow at
   most one policy-approved character behavior.
6. `_build_normal_response_context` prepares inputs for the unchanged
   `get_response()` implementation.
7. `_generate_normal_reply` performs the one normal model call.
8. `_apply_post_response_effects` performs best-effort relationship mutations.
9. `_deliver_normal_reply` selects voice or text, records successful delivery,
   then runs best-effort edit/reaction behavior.

`PreparedMessage` in `message_pipeline.py` contains runtime data. It does not
duplicate routing, safety, consent, or ownership decisions from
`InteractionContext`.

## Consumption semantics

Only the current owner may call `consume()` or `select()`. Once an interaction is
consumed or suppressed, later owners cannot respond. Structured sessions are
resolved in `_dispatch_message()` before the runtime pipeline. Media and optional
features return a consumed result to the coordinator; normal generation runs only
when all earlier stages decline ownership.

Observations are deliberately separate from ownership. A successful world or
message-count write does not claim a response, and a non-critical persistence
failure does not invent a second response owner.

## Critical and best-effort work

Critical work includes classification, consent/boundary enforcement, structured
session ownership, and delivery. Unexpected failures propagate to the single
`on_message()` safety boundary and are logged with a traceback.

Best-effort work includes optional relationship mutations, cosmetic reactions,
and opt-in delayed edits. These operations may fail without discarding a delivered
answer, but their failures are categorized and logged. Optional effects never retry
Discord operations in an uncontrolled loop.

## Error categories

`message_pipeline.log_operation_error()` emits identifiers and categories without
message bodies:

- `discord_forbidden`: permission failure; stop the optional operation.
- `discord_not_found`: stale/deleted Discord object; stop the operation.
- `discord_http`: other Discord API failure; rely on discord.py's rate-limit
  behavior and do not add a router retry loop.
- `timeout`: attachment or network operation exceeded its bound.
- `sqlite_operational`: an operational persistence failure such as a locked DB.
- `sqlite_integrity`: a constraint or consistency failure.
- `unexpected`: programming/invariant failure, logged with a traceback.

Logs include operation, subsystem, exception type, message/user/channel/guild IDs,
Discord status when present, and the selected response path. They do not include
DM text, message content, credentials, face data, screen summaries, or transcripts.

## Cancellation

`asyncio.CancelledError` always propagates through message stages, media work,
generation, delivery, reactions, and delayed edits. The top-level Discord boundary
also re-raises it. Cancellation is shutdown/control flow, not a generic failure or
a reason to produce an in-character fallback.

## Delivery and memory ordering

Normal text and completed media replies are written to assistant memory only after
Discord confirms the send. A failed `reply()` is categorized and is not recorded
as delivered. If Fish Audio fails an explicit voice attempt, the existing text
fallback remains; successful voice sends retain the existing voice-memory marker.
Post-response relationship writes happen independently and cannot suppress the
main delivery.

## Reply-reference and media behavior

Resolved or discord.py-cached references are reused. An unresolved reference is
fetched once only when a reply needs routing, partner, media, or voice context; the
result is stored in `PreparedMessage` for the rest of the interaction. Deleted and
inaccessible references become stale context rather than crashing the message.
Referenced media can now be handled even when the new reply contains no text.

Media failures distinguish oversize input, download timeout/staleness, frame
extraction, provider generation, and Discord delivery. Existing size limits,
prompts, response probabilities, and voice probabilities are unchanged.

## Deliberately remaining broad boundaries

This batch does not refactor `get_response()`, background integrations, home-agent
internals, the complete voice-conversation subsystem, or every command handler.
Broad catches remain in those out-of-scope areas, in the one top-level Discord
safety boundary, and in a few explicitly best-effort adapters. They are candidates
for later focused audits, not silent justification for message-pipeline failures.
