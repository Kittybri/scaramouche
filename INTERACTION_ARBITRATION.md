# Interaction Arbitration

`interaction_policy.py` is the deterministic response-time authority for Discord
interactions. It does not call a model, access the network, or write persistence.
The runtime classifies a message once, enriches that context with the loaded user
preferences and active-session state, and passes the same object through routing
and response generation.

## Precedence

The effective order is:

1. self/bot-loop and duplicate-message suppression;
2. credential-disclosure protection, then explicit command dispatch;
3. user boundary/mute and serious or accuracy-first response handling;
4. participant-scoped structured sessions (interview, court, wager, voice game,
   trivia, then ordinary duo mode);
5. explicit DM, mention, or reply-to-this-bot interaction;
6. persistent-world observation and media handling;
7. at most one optional pre-response character feature;
8. the normal character response; and
9. existing bounded post-response behavior.

Cancellation, restore, privacy, status, and help controls remain available when a
serious message would otherwise block an optional game command.

## Classification and serious context

`InteractionContext` carries the reused `classify_safety()` result, response mode,
command/direct/media status, preferences, active owner, selection, and outcome.
`NORMAL`, `UTILITY`, `SERIOUS`, and `HIGH_STAKES_UTILITY` make accuracy and the
current need authoritative without turning Scaramouche into a different character.

Serious and accuracy-first contexts suppress comedy, callbacks, selective hearing,
trivia/pop quizzes, random triggers, server-chaos parody, trolling, fake typing,
Scapegoat, and petty grudge behavior. A short guild-scoped pause also prevents
sibling typing listeners from firing immediately around a protective interaction.

## Ownership and optional features

An interaction begins as `CONTINUE` and may become `CONSUMED` or `SUPPRESSED`.
Only the first consuming response path wins. `select()` combines optional-feature
eligibility with consumption, so independent random checks cannot produce multiple
preemptive replies. Feature modules still own their Discord permissions, cooldowns,
consent checks, and budgets.

Structured sessions are channel- and participant-scoped. A participant's active
session owns their ordinary input; unrelated people in the channel are not captured
by an interview or trivia question.

## Character-state precedence

`ResolvedCharacterState` interprets, rather than replaces, stored mood, affection,
trust, conflict, grudge, and self-model dimensions. Its compact prompt directive is
appended after raw context so these rules are authoritative:

- safety and current need outrank comedy, irritation, and grudge;
- boundaries, opt-out, consent, and permission outrank attachment or willingness;
- unresolved conflict shapes cadence but cannot override the above;
- relationship warmth is user-scoped, not inherited from global attachment;
- a structured role outranks random character behavior; and
- the current event outranks stale callbacks.

## Observability and tests

The dispatcher logs mode, serious flag, command flag, session owner, selected
feature, suppression count, response path, and outcome without message content.
`tests/test_interaction_policy.py` covers serious keyword collisions, optional
feature families, command/session ownership, participant scoping, one-owner
consumption, boundary and relationship precedence, credential protection, sibling
listener gating, identity preservation, and prompt structure.
