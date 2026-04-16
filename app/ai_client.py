"""
Centralized AI client factory.

Supports two providers controlled by AI_PROVIDER env var:
  - "openai"  (default) — uses OPENAI_API_KEY, models like gpt-5-nano
  - "nvidia"  — uses NVIDIA_API_KEY + NVIDIA_BASE_URL, OpenAI-compatible endpoint

Both providers use the ``openai`` Python SDK; NVIDIA's inference hub exposes
an OpenAI-compatible ``chat.completions`` API.

Note: NVIDIA does NOT support the OpenAI Responses API (``responses.create``).
All callers must use ``chat.completions.create`` for cross-provider compat.
"""
from __future__ import annotations

import logging
from functools import lru_cache

from openai import OpenAI

from .config import get_settings

logger = logging.getLogger(__name__)


def _provider() -> str:
    s = get_settings()
    return (s.ai_provider or "openai").strip().lower()


@lru_cache(maxsize=1)
def get_ai_client(*, timeout: float = 60.0) -> OpenAI:
    """Return a configured OpenAI client for the active provider."""
    s = get_settings()
    provider = _provider()

    if provider == "nvidia":
        if not s.nvidia_api_key:
            logger.warning("[ai] NVIDIA provider selected but NVIDIA_API_KEY is empty")
        return OpenAI(
            api_key=s.nvidia_api_key,
            base_url=s.nvidia_base_url,
            timeout=timeout,
        )

    if not s.openai_api_key:
        logger.warning("[ai] OpenAI provider selected but OPENAI_API_KEY is empty")
    return OpenAI(
        api_key=s.openai_api_key,
        timeout=timeout,
    )


def get_model(*, default: str = "gpt-5-nano") -> str:
    """Return the model name for the active provider."""
    s = get_settings()
    if _provider() == "nvidia":
        return s.nvidia_model or "aws/anthropic/bedrock-claude-opus-4-6"
    return default


def is_nvidia() -> bool:
    return _provider() == "nvidia"


def has_ai_key() -> bool:
    """Return True if the active provider has an API key configured."""
    s = get_settings()
    if _provider() == "nvidia":
        return bool(s.nvidia_api_key)
    return bool(s.openai_api_key)
