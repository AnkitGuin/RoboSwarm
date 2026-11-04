# Data contracts

## Telemetry schema

Required: `event_id` UUID; `robot_id`, `session_id` nonempty strings; `timestamp` ISO8601 timezone-aware datetime converted to UTC; `sequence_number` nonnegative int; `event_type` one of `observation`, `action`, `state`, `task_event`.

Optional: `joint_positions` and `joint_velocities` float arrays of profile-configured joint count; `end_effector_position` 3 floats in metres; `end_effector_orientation` quaternion 4 floats ordered x,y,z,w; `gripper_position` float within profile bounds; `gripper_state` `open/closed/moving/unknown`; `action` arbitrary JSON object; `task_id`; `task_status` `running/success/failure/aborted`; `camera_frame_ref` string; `metadata` free-form JSON object.

Task events require `task_id`; status only appears on task events. Action events must have an `action` object. Observation sensor values may be null individually. Unknown fields are rejected rather than ignored. Naive timestamps are rejected. Any non-finite values are physically invalid and rejected before Parquet. Generator degraded NaN may be encoded with JSON's non-standard `NaN` token for fault injection; Python JSON ingestion recognizes it, while strict external JSON tools may reject it. The original bytes are archived regardless.

## Keys and ordering

An `event_id` is globally unique across all batches; duplicate `event_id`s are rejected and flagged. The source line number indexes the immutable raw archive. A session has a deterministic UUID5 key computed from both `robot_id` and `session_id` with length-prefixed encoding (avoids ambiguities from colon-containing identifiers); clients query by `session_id` with optional `robot_id` to disambiguate. An episode is a UUID5 from the sorted event ID sequence, robot/session/task, and an effective configuration hash (includes profile). Force-reprocessed runs retain the stable logical episode ID but are stored as separate database episode rows with separate Parquet paths. The API lists the latest revision by default; `pipeline_run_id` exposes one revision.

Normalized Parquet typed fields are: event_id, robot_id, session_id, task_id, UTC timestamp (microseconds), int64 sequence_number, event_type, task_status, float64 lists for joint positions/velocities/end-effector position/quaternion, float64 gripper_position, gripper_state, camera_frame_ref, action_json, metadata_json. Compression Zstd. No raw payload is overwritten.

## Error codes

`MISSING_REQUIRED_FIELD`, `INVALID_TIMESTAMP`, `SCHEMA_VALIDATION_ERROR`, `DUPLICATE_EVENT_ID`, `DUPLICATE_SEQUENCE_NUMBER`, `OUT_OF_ORDER_TIMESTAMP`, `OUT_OF_ORDER_SEQUENCE`, `MISSING_SEQUENCE_NUMBER`, `DUPLICATE_TIMESTAMP`, `TIMESTAMP_GAP`, `INCONSISTENT_SAMPLING_RATE`, `NON_FINITE_SENSOR_VALUE`, `JOINT_COUNT_MISMATCH`, `JOINT_LIMIT_EXCEEDED`, `VELOCITY_LIMIT_EXCEEDED`, `WORKSPACE_LIMIT_EXCEEDED`, `INVALID_QUATERNION`, `GRIPPER_LIMIT_EXCEEDED`, `CONFLICTING_TASK_STATUS` and `INCOMPLETE_EPISODE` (segmentation flag). Severities: info/warning/error. Schema rejection and physical errors are fatal for the individual record; temporal warnings do not cause rejection.
