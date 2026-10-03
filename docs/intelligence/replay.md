# Replay and the no-look-ahead guarantees

Replay runs an archived session through the engine minute by minute, exactly
as the live service would have.

```bash
python -m intelligence days                                   # archived sessions
python -m intelligence replay 2026-10-20 --until 10:17         # data quality as known at 10:17
python -m intelligence run 2026-10-20                          # every engine, every minute; events stored
python -m intelligence run 2026-10-20 --every 5 --store-dir /tmp/scratch
```

`run` prints the number of steps, the events by type and the time per stage.
Events go to `<store>/events.db`; ids are deterministic, so running a day the
live service already processed adds nothing.

## What a frame at `as_of` contains

- 1m bars whose bar has *closed* by `as_of` (bar open + 60 s ≤ as_of), from 09:15 to 15:15 (`ANALYSIS_END`; PSYGRID records to 15:30, the research window stops at 15:15 so every model and verdict stays comparable);
- rejected rows only once their bar has closed;
- the previous close and today's open (known before the session).

## Guarantees, and the tests that hold them

| Guarantee | Test |
| --- | --- |
| A frame never contains a later bar | `test_foundation.py` |
| Baselines use only sessions before the date; changing the day being judged changes nothing | `test_features.py::test_baselines_ignore_the_day_being_judged` |
| Every engine's output at `t` is identical when every bar and derivatives snapshot after `t` is replaced by wild values | `test_pipeline.py::test_no_engine_sees_the_future` (four checkpoints) |
| A relationship judgement at `t` ignores later returns | `test_relationships.py::test_judgement_ignores_the_future` |
| Similarity never searches later sessions | `test_similarity.py::test_later_sessions_are_never_searched` |
| A past minute's state equals what a frame at that minute showed | `test_similarity.py::test_a_past_minute_state_is_what_a_frame_at_that_minute_showed` |
| A full session replayed twice gives identical event ids | `test_pipeline.py::test_full_session_replay_is_deterministic` |
| Live (following the archive) gives the same events as replay | `test_service.py::test_live_follows_the_archive_and_matches_replay` |
| Live payloads give the same frame as the archive | `test_pipeline.py::test_live_payloads_give_the_same_frame_as_the_archive` |

## Cadence matters for events

Cooldown is measured in bar time, so `--every 1` (what live does) and
`--every 5` give different event sets. To reproduce live events, replay with
`--every 1`.
