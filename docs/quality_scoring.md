# Explainable episode quality heuristic

Let `N` be the number of valid records in an episode, `O` its number of observation events, and `F` the five monitored fields (joints, velocities, end-effector position, orientation, gripper position).

`missing_rate_f = missing observations of field f / max(O, 1)` (or zero for zero observations). `M = mean_f(missing_rate_f)`.

Let `T` be count of temporal issue events, `D` duplicate event/sequence count, `I` physical/numerical invalid issue count associated with the same session/task, including dropped readings. Penalties:

```
p_missing = missing_weight * M                                      (default 30)
p_timestamp = timestamp_weight * min(1, T / max(N,1))               (default 20)
p_duplicate = duplicate_weight * min(1, D / max(N,1))               (default 15)
p_invalid = invalid_weight * min(1, I / max(N,1))                   (default 30)
p_incomplete = incomplete_penalty if no terminal event else 0       (default 20)
score = clamp(100 - sum(penalties), 0, 100)
```

Labels: `excellent` (90–100), `acceptable` (75–<90), `poor` (50–<75), `rejected` (<50). Any episode without an observation is hard-failed as `rejected` independent of numeric score.

This is **not** an empirically validated quality metric. Penalties are configurable using `configs/default.yaml`, and are normalized by record count to avoid punishing duration alone. Physical issues for rejected samples may be attributed to the same task across the session (not exactly within interval), and quality-score interpretability is prioritized over a probabilistic fitness claim. Production refinements could include per-sensor severity weighting, task-specific expected missingness and temporal alignment of excluded records.

The report exposes penalties, `quality_flags`, `issue_counts`, missing rates, max time gaps, sampling frequency estimated as the inverse median of positive successive **observation** intervals, completion status, record count, duration and source event IDs. Task/action/state markers do not participate in sensor cadence estimates; equal timestamps between such markers and observations are therefore not reported as duplicate sensor samples. Arrival-order timestamp and sequence regressions still inspect all event types.

**Example**: for a complete episode with a 10% average missing-sensor rate, no other issues, the score is `100 - 30*0.10 = 97` (`excellent`). If terminal status is missing, the score is 77 (`acceptable`) before any other penalties. No ML model is used.
