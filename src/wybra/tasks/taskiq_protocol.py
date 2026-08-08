"""Private protocol values shared by Taskiq adapters."""

DELIVERY_ATTEMPT_LABEL = "_wybra_task_delivery_attempt"
DELIVERY_IDENTITY_LABEL = "_wybra_task_delivery_identity"
DELIVERY_RECEIPT_LABEL = "_wybra_task_delivery_receipt"
TASK_CAUSATION_ID_LABEL = "_wybra_task_causation_id"
TASK_CORRELATION_ID_LABEL = "_wybra_task_correlation_id"
TASK_IDEMPOTENCY_KEY_LABEL = "_wybra_task_idempotency_key"
TASK_NAME_LABEL = "_wybra_task_name"
TASK_QUEUE_LABEL = "_wybra_task_queue"
TASK_RETRY_BACKOFF_MULTIPLIER_LABEL = "_wybra_task_retry_backoff_multiplier"
TASK_RETRY_INITIAL_DELAY_LABEL = "_wybra_task_retry_initial_delay_seconds"
TASK_RETRY_JITTER_SECONDS_LABEL = "_wybra_task_retry_jitter_seconds"
TASK_RETRY_MAXIMUM_DELAY_LABEL = "_wybra_task_retry_maximum_delay"
TASK_SCHEMA_VERSION_LABEL = "_wybra_task_schema_version"
TASK_VISIBILITY_TIMEOUT_LABEL = "_wybra_task_visibility_timeout"

__all__ = (
    "DELIVERY_ATTEMPT_LABEL",
    "DELIVERY_IDENTITY_LABEL",
    "DELIVERY_RECEIPT_LABEL",
    "TASK_CAUSATION_ID_LABEL",
    "TASK_CORRELATION_ID_LABEL",
    "TASK_IDEMPOTENCY_KEY_LABEL",
    "TASK_NAME_LABEL",
    "TASK_QUEUE_LABEL",
    "TASK_RETRY_BACKOFF_MULTIPLIER_LABEL",
    "TASK_RETRY_INITIAL_DELAY_LABEL",
    "TASK_RETRY_JITTER_SECONDS_LABEL",
    "TASK_RETRY_MAXIMUM_DELAY_LABEL",
    "TASK_SCHEMA_VERSION_LABEL",
    "TASK_VISIBILITY_TIMEOUT_LABEL",
)
