"""
Centralized AI client factory.

Supports two providers controlled by AI_PROVIDER env var:
  - "nvidia" (default) — uses NVIDIA_API_KEY + NVIDIA_BASE_URL, OpenAI-compatible
    endpoint. Default heavy model is Claude Opus 4.6 via AWS Bedrock; default
    fast model is Claude Haiku 4.5.
  - "openai"  — uses OPENAI_API_KEY, models like gpt-5-nano

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


_DEFAULT_AI_TIMEOUT = 60.0


@lru_cache(maxsize=1)
def get_ai_client() -> OpenAI:
    """Return a configured OpenAI client for the active provider.

    The client is cached for the lifetime of the process. Callers that need a
    different request timeout should use ``get_ai_client().with_options(timeout=X)``
    rather than creating a new client — ``lru_cache`` keyed on timeout caused
    thrashing when different call sites passed different values.
    """
    s = get_settings()
    provider = _provider()

    if provider == "nvidia":
        if not s.nvidia_api_key:
            logger.warning("[ai] NVIDIA provider selected but NVIDIA_API_KEY is empty")
        return OpenAI(
            api_key=s.nvidia_api_key,
            base_url=s.nvidia_base_url,
            timeout=_DEFAULT_AI_TIMEOUT,
        )

    if not s.openai_api_key:
        logger.warning("[ai] OpenAI provider selected but OPENAI_API_KEY is empty")
    return OpenAI(
        api_key=s.openai_api_key,
        timeout=_DEFAULT_AI_TIMEOUT,
    )


def get_model(*, default: str = "gpt-5-nano") -> str:
    """Return the model name for the active provider."""
    s = get_settings()
    if _provider() == "nvidia":
        return s.nvidia_model or "aws/anthropic/bedrock-claude-opus-4-6"
    return default


def get_fast_model(*, default: str = "gpt-5-nano") -> str:
    """Return a fast/cheap model for lightweight tasks (query parsing, etc.)."""
    s = get_settings()
    if _provider() == "nvidia":
        return s.nvidia_fast_model or "aws/anthropic/claude-haiku-4-5-v1"
    return default


def is_nvidia() -> bool:
    return _provider() == "nvidia"


def has_ai_key() -> bool:
    """Return True if the active provider has an API key configured."""
    s = get_settings()
    if _provider() == "nvidia":
        return bool(s.nvidia_api_key)
    return bool(s.openai_api_key)


# ---------------------------------------------------------------------------
# Tiebreaker (ensemble third-opinion model)
#
# Used only when Ximilar and the primary vision model disagree on a scan. The
# tiebreaker is currently routed through the same NVIDIA Inference Hub client
# so it shares the existing auth path; the only thing that changes is the
# model id (defaults to Gemini 3.1 Pro preview hosted on NVIDIA).
# ---------------------------------------------------------------------------

def get_tiebreaker_client() -> OpenAI:
    """Return an OpenAI-compatible client to use for the tiebreaker call.

    Routed through the existing NVIDIA client today. Kept as its own function
    so callers remain semantically clear and the provider can be swapped in
    one place later without touching call sites.
    """
    return get_ai_client()


def get_tiebreaker_model() -> str:
    """Return the model id used for tiebreaker calls."""
    s = get_settings()
    return s.nvidia_tiebreaker_model or "gcp/google/gemini-3.1-pro-preview"


def has_tiebreaker_key() -> bool:
    """Return True when the tiebreaker is usable with the current config.

    Today the tiebreaker rides the NVIDIA endpoint, so availability is tied to
    ``is_nvidia()`` + ``NVIDIA_API_KEY`` being set.
    """
    s = get_settings()
    return is_nvidia() and bool(s.nvidia_api_key)
