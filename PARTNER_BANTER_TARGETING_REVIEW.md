# Partner banter: no unsolicited human target

## Screenshot regression
Wanderer's unsolicited channel prompt asked people for their "worst
opinion." Scaramouche then publicly tagged a romance-mode person and
insulted a fictional opinion as though the person had stated it.

## Confirmed code path
`_handle_partner_message()` observed otherwise unowned partner-bot
speech. If a romance-mode user was present in the channel, it augmented
the prompt with their display name and, with probability 45%, prefixed
their real Discord mention to Scaramouche's generated reply. This is
not evidence the person consented to be roasted or even participated.

## Changes
- Keep existing bot-to-bot rivalry and jealousy *mood*, but do not give
  the model a bystander's name or ping any real person.
- Reply to Wanderer's message only. No surprise channel-level user pings.
- Prompt clearly prohibits inventing a bystander's opinion and redirects
  teasing back to the partner bot.
- Drop any optional model-generated rival banter containing an @ mention
  instead of allowing accidental user, role or everyone pings.
- Preserve existing skip of messages directly targeting a human, rich
  command/media outputs, cooldowns, duo-session ownership, and character
  relationship updates for delivered replies.
- Add mocked async tests reproducing the unsolicited prompt with a
  romance-mode bystander and a generated ping.

## Validation
Run preservation and targeted message-pipeline checks, then full suites,
then owner-approved real Discord two-bot testing. No voice or Google
code was changed. No production/release merge or deployment performed.

## Follow-up targeting clarification
The romance-mode user may be referred to **in the third person** (without @ ping) to support an intelligible jealousy joke aimed at Wanderer. All organic banter now uses a real Discord message reply to the partner, with the partner explicitly identified in the generated text. The duo autoplay worker also replies to its latest partner message when one occurs after the selected participant's message, falling back to replying to the relevant human participant for the initial handoff. It no longer posts unanchored standalone turns. Untrusted human display names are sanitized before going into the jealousy prompt. These changes retain romance flavor while keeping the speaker and addressee distinct.

## Scope correction: restore the behavior the owner requested
The initial PR disabled the old **45% romance-mode mention chance**, suppressed
all model-generated `@` mentions and forced every reply to address Wanderer.
Those restrictions were not requested and are now removed. The original
probability and existing jealousy/competitive character flavor are retained.
When the optional romance ping is selected, it is used naturally *inside*
the jealous remark or a short coherent follow-up rather than incorrectly
prefixed as the target of a statement by Wanderer. Discord replies continue
to reference Wanderer's actual message. The only new targeting rule is that
Scaramouche must not falsely attribute Wanderer's speech to the romance
user. The overall banter frequency, relationship systems and voice remain
unchanged.
