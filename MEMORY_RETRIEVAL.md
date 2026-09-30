# Memory retrieval and response variety

Normal response assembly uses one bounded retrieval pass in `memory_retrieval.py`. It performs no provider or embedding calls and does not add a persistent profiling table.

## Candidate sources

For the current user only, `Memory.get_memory_retrieval_snapshot` reads a bounded set of:

- up to 16 memory-bank entries;
- five topics;
- eight local and eight shared inside jokes;
- four relationship milestones;
- twelve historical user messages older than two days from the **current channel only**.

The response layer may add the same user's summary, callback, active conflict, and up to three continuity hooks from the already-bounded conversation history. All sources enter one arbitration pass. Topics are background candidates, not instructions to mention a favorite subject.

## Scoring

Text is lowercased, stripped of punctuation and common stop words, lightly suffix-normalized, and mapped through a small alias table such as `anxious -> nervous` and `job -> interview`.

Each candidate receives:

- lexical coverage/Jaccard relevance;
- bounded memory weight/importance;
- age-based recency;
- kind-to-query intent relevance for actual stored kinds such as promise, fight, repair, vulnerability, comfort, and inside joke;
- emotional relevance;
- relationship relevance when supported by the text or a specific kind;
- unresolved-conflict relevance when the current message also supports it;
- novelty based on persistent `memory_bank.last_used` or bounded runtime use.

Penalties apply to very recent reuse, serious-context jokes, stale low-value records, and candidates with no relevance signal. The current relevance floor is **0.38**. If nothing reaches it, no episodic memory is injected.

The highest-quality candidates are deduplicated by token overlap and text similarity. Selection varies only among the top three candidates within 0.16 of the best score. A clearly weaker or irrelevant memory never enters the random pool. At most **two** memory fragments enter a response, with mild preference for different memory kinds when scores are close.

`last_used` changes only for selected memory-bank rows and only after their fragments are present in the assembled provider prompt. Considered, rejected, deduplicated, or later-unassembled candidates are not marked. Other source reuse is suppressed in a seven-day, 4,096-entry process-only cache.

## Callbacks, jokes, and history

Callbacks and milestones now compete on relevance instead of independent random gates. A generic “remember” cannot summon an unrelated relationship event. Active conflict boosts only conflict material supported by the current message. Inside jokes remain available in playful, relevant contexts and are ineligible during serious interactions.

Random old-message recall was removed from normal response paths. Historical recall is relevance-scored, user-scoped, bot-scoped, and restricted to the current channel. This prevents a DM line or another channel's message from becoming a public callback.

## Rhetorical anti-repeat

`anti_repeat.py` retains exact, opening, SequenceMatcher, phrase-cooldown, and structural checks. Its response signature now also tracks:

- opening family;
- rhetorical pattern;
- sentence count and length band;
- answer position;
- question placement;
- mockery position;
- admission/softness.

The bounded deterministic classifier recognizes these recurring moves:

- fake praise;
- sarcastic congratulations;
- dismissive or one-word openings;
- insult-then-answer and answer-then-insult;
- rhetorical disbelief and mock questions;
- reluctant help and reluctant care;
- denial after softness;
- rank flexes;
- creator-wound callbacks;
- dramatic threats.

A pattern is not rejected after one appearance. It becomes stale after three prior uses for the same user, four in the same channel, or eight globally. User scope is strongest and global scope is deliberately weakest. Runtime user history holds at most 24 signatures across 2,048 active users for seven days; channel history holds 32 signatures across 1,024 channels for two days; global history holds 80 signatures per character.

Global runtime pattern history stores pattern names, not user reply text. Persisted assistant messages remain available for exact textual checks, but prompt guards expose only generic warnings such as “fake praise,” never another user's text. User-scoped signatures and retrieval reuse markers are cleared by the existing privacy reset runtime stage.

Factual and serious modes disable generic shape rejection and only suppress clearly stale stylistic habits such as fake praise, dismissive openings, one-word dismissals, and sarcastic congratulations. Direct factual structure and appropriate supportive structure remain valid.

Rhetorical rejection uses an existing response attempt. `CONFIG.response_attempts` is unchanged: an acceptable first draft still costs one provider call, the configured maximum is unchanged, and memory retrieval adds **zero** provider calls.

## Diagnostics and limitations

Debug logs contain counts, source/kind labels, numeric scores, pattern names, frequencies, rejection booleans, and retry number. Raw memory and private reply text are not logged.

This is lexical retrieval, so paraphrases without shared normalized concepts can still be missed. Embeddings could improve those cases later, but are intentionally not implemented here. Runtime novelty and rhetorical histories reset with the process; persistent memory-bank `last_used` survives restart.
