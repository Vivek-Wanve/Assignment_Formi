import asyncio
import logging
import time
from collections import defaultdict
from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

class LLMScheduler:
    def __init__(self):
        self.global_tokens_per_minute = settings.LLM_TOKENS_PER_MINUTE
        self.global_requests_per_minute = settings.LLM_REQUESTS_PER_MINUTE
        self.avg_tokens_per_call = settings.LLM_AVG_TOKENS_PER_CALL
        self.per_customer_budget = defaultdict(lambda: self.global_tokens_per_minute // 10)  # default 10% each

    async def acquire_slot(self, customer_id: str, tokens_needed: int) -> bool:
        """
        Try to acquire a slot for this customer.
        Returns True if allowed, False if must defer.
        """
        now = int(time.time())
        key_global = f"llm:tokens:{now//60}"
        key_customer = f"llm:tokens:{customer_id}:{now//60}"

        # Get current usage
        global_used = int(await redis_client.get(key_global) or 0)
        customer_used = int(await redis_client.get(key_customer) or 0)

        # Check limits
        if global_used + tokens_needed > self.global_tokens_per_minute:
            logger.warning("llm_global_limit_reached", extra={"customer_id": customer_id})
            return False

        if customer_used + tokens_needed > self.per_customer_budget[customer_id]:
            logger.warning("llm_customer_limit_reached", extra={"customer_id": customer_id})
            return False

        # Reserve tokens
        await redis_client.incrby(key_global, tokens_needed)
        await redis_client.expire(key_global, 120)
        await redis_client.incrby(key_customer, tokens_needed)
        await redis_client.expire(key_customer, 120)

        return True

llm_scheduler = LLMScheduler()
