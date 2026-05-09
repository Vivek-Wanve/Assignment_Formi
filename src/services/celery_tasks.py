import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext, post_call_processor
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.retry_queue import retry_queue
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main Celery task. Updated to run recording and LLM analysis in parallel.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except Exception as e:
        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error": str(e),
                "attempt": self.request.retries,
            },
        )
        loop.run_until_complete(
            retry_queue.enqueue_retry(
                interaction_id=payload["interaction_id"],
                error=str(e),
                payload=payload,
            )
        )
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]):
    ctx = PostCallContext(
        interaction_id=payload["interaction_id"],
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )

    await metrics_tracker.track_processing_started(ctx.interaction_id)

    # ── Step 1 + 2: Run recording poller and LLM analysis in parallel ─────────
    recording_task = asyncio.create_task(
        fetch_and_upload_recording(ctx.interaction_id, ctx.call_sid, ctx.exotel_account_id or "")
    )
    llm_task = asyncio.create_task(post_call_processor.process_post_call(ctx, single_prompt=True))

    recording_s3_key, result = await asyncio.gather(recording_task, llm_task, return_exceptions=True)

    if isinstance(recording_s3_key, Exception):
        logger.error("recording_failed", extra={"interaction_id": ctx.interaction_id})
    elif recording_s3_key:
        logger.info("recording_uploaded", extra={"interaction_id": ctx.interaction_id, "s3_key": recording_s3_key})

    if isinstance(result, Exception):
        logger.error("llm_failed", extra={"interaction_id": ctx.interaction_id})
        raise result

    await metrics_tracker.track_processing_completed(
        ctx.interaction_id, result.tokens_used, result.latency_ms
    )

    # ── Step 3: Signal jobs ───────────────────────────────────────────────────
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
    except Exception as e:
        logger.warning("signal_jobs_failed", extra={"error": str(e)})

    # ── Step 4: Lead stage update ─────────────────────────────────────────────
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
    except Exception as e:
        logger.warning("lead_stage_update_failed", extra={"error": str(e)})
