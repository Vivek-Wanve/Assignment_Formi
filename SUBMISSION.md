Assumptions
Campaigns can generate 100K+ calls in a few hours.

LLM provider enforces hard rate limits (tokens/minute, requests/minute).

Customers may have different priorities (e.g., enterprise vs SMB).

Some calls are urgent (confirmed booking, angry lead) and must be processed immediately; others (wrong number, hangups) can be deferred.

Exotel recordings may take 10–120s to appear, but are poll-friendly.

Business requires no silent drops — every call must be traceable.

Architecture Overview
Flow:

Webhook → FastAPI endpoint

Marks interaction as ENDED.

Decides short vs long transcript.

Enqueues durable task (Kafka/RabbitMQ, not Redis-only).

Task Queue (Kafka/RabbitMQ)

Durable, partitioned by customer.

Supports retries, dead-letter queues.

Processing Workers

Two parallel pipelines:

Recording Poller (poll Exotel until recording available, upload to S3).

LLM Scheduler (rate-limit aware, token-budget enforced).

LLM Scheduler

Maintains global token/request budget.

Allocates per-customer quotas.

Queues deferred calls.

Urgent calls bypass queue (within budget).

Post-Processing

Update dashboard (interaction_metadata).

Trigger signal jobs (CRM, WhatsApp, callbacks).

Update lead stage.

Observability Layer

Structured logs with correlation IDs.

Metrics: queue depth, TPM/RPM usage, per-customer tokens.

Alerts: retry exhaustion, backlog > threshold, recording failures.

Rate Limit Management
Token-aware scheduler:

Tracks tokens/minute in Redis or Postgres.

Uses sliding window counters.

Urgency classification:

Calls with confirmed bookings, callbacks, or escalations → immediate.

Wrong numbers, short calls → deferred.

Graceful recovery:

If 429 received, respect Retry-After header.

Exponential backoff for retries.

Deferred queue drains when headroom exists.

Per-Customer Token Budgeting
Each customer has a pre-allocated quota (e.g., 20% of total).

Scheduler enforces:

Customer A cannot consume Customer B’s allocation.

Unused quota is pooled and redistributed fairly.

If a customer exceeds quota:

Non-urgent calls deferred.

Urgent calls may consume pooled headroom.

Recording Pipeline Fix
Replace asyncio.sleep(45s) with poller:

Poll Exotel every 10s up to 120s.

Retry with exponential backoff.

Log every attempt.

If recording never arrives → structured error + alert.

Run recording fetch in parallel with LLM analysis.

Reliability & Durability
Replace Celery + Redis with Kafka/RabbitMQ + Postgres:

Durable queues.

Dead-letter queues for exhausted retries.

Replay capability.

Every task carries correlation ID.

No fire-and-forget — all downstream jobs acked or retried.

Auditability & Observability
Structured logs:

interaction_id, customer_id, campaign_id, step, status.

Metrics:

p95 latency, backlog depth, per-customer tokens.

Alerts:

Retry exhaustion.

Backlog > threshold.

Recording failures.

Data Model Changes
Add llm_tokens_used (int).

Add processing_status (enum: pending, in_progress, completed, failed).

Add error_log (JSONB with structured errors).

Add priority (enum: urgent, normal, deferred).

Security
Sensitive data: transcripts, recordings, PII.

Protect with:

Encryption at rest (Postgres, S3).

TLS in transit.

Access controls (per-customer isolation).

Trade-offs
Kafka adds infra complexity vs Redis.

Scheduler adds latency for deferred calls.

Urgency classification may misclassify some calls.

Parallel pipelines increase concurrency complexity.