import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass

from src.config import settings
from src.services.circuit_breaker import circuit_breaker
from src.services.llm_scheduler import llm_scheduler   # NEW
from src.services.retry_queue import retry_queue       # NEW

logger = logging.getLogger(__name__)


@dataclass
class PostCallContext:
    """Everything needed to process one completed call."""
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str
    agent_id: str
    call_sid: str
    transcript_text: str
    conversation_data: dict
    additional_data: dict
    ended_at: datetime
    exotel_account_id: Optional[str] = None


class PostCallProcessor:
    async def process_post_call(
        self, ctx: PostCallContext, single_prompt: bool = True
    ):
        # Scheduler check before firing LLM
        allowed = await llm_scheduler.acquire_slot(
            ctx.customer_id, settings.LLM_AVG_TOKENS_PER_CALL
        )
        if not allowed:
            logger.info(
                "llm_request_deferred",
                extra={"interaction_id": ctx.interaction_id, "customer_id": ctx.customer_id},
            )
            # Defer: push to retry queue with short delay
            await retry_queue.enqueue_retry(
                ctx.interaction_id, "Rate limit exceeded", ctx.__dict__
            )
            return None

        # Record request start
        await circuit_breaker.record_postcall_start()

        try:
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                single_prompt,
            )

            start_time = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            result = self._parse_response(response, elapsed_ms)

            await self._update_interaction_metadata(ctx.interaction_id, result)

            logger.info(
                "postcall_analysis_complete",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "call_stage": result.call_stage,
                    "tokens_used": result.tokens_used,
                    "latency_ms": result.latency_ms,
                    "priority": ctx.additional_data.get("priority", "normal"),
        "processing_status": "completed",
                },
            )

            return result

        except Exception as e:
            logger.exception(
                "postcall_analysis_failed",
                extra={"interaction_id": ctx.interaction_id, "error": str(e)},
            )
            raise

        finally:
            await circuit_breaker.record_postcall_end()

    # ... existing helper methods (_build_analysis_prompt, _call_llm, _parse_response, _update_interaction_metadata) remain unchanged ...
