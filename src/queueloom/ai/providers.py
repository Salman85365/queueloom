from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Protocol

log = logging.getLogger("queueloom.ai")

DEFAULT_MODEL = "claude-opus-5"

SYSTEM_PROMPT = """You are an on-call engineer's assistant for QueueLoom, an observability tool \
for Python background jobs (Celery and similar). You receive a diagnosis report computed from \
stored task telemetry: failure rates versus a baseline window, error clusters with example \
messages and one sample traceback, latency and duration changes, and stuck tasks.

Write for an engineer who is about to investigate. Use only facts in the report; if the report \
does not contain enough information to decide something, say what is missing instead of guessing.

Respond in this structure, in plain prose with short paragraphs and lists, no headings larger \
than a bold label:
1. **What is happening** - two or three sentences.
2. **Likely causes** - ranked, each with the evidence from the report that supports it.
3. **Next steps** - concrete checks or fixes, most valuable first.
4. **Not enough data for** - anything you could not determine, if applicable.

Keep the whole answer under 300 words unless the report has several unrelated problems."""


class SummaryError(RuntimeError):
    """Raised when a provider cannot produce a summary; message is safe to show users."""


@dataclass(frozen=True)
class Summary:
    text: str
    provider: str
    model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
        }


class SummaryProvider(Protocol):
    name: str

    def summarize(self, report: str, *, question: str | None = None) -> Summary: ...


class TemplateProvider:
    """No model configured: return the deterministic report as the summary."""

    name = "template"

    def summarize(self, report: str, *, question: str | None = None) -> Summary:
        return Summary(text=report, provider=self.name)


def build_user_message(report: str, question: str | None) -> str:
    text = f"<diagnosis_report>\n{report}\n</diagnosis_report>"
    if question:
        text += f"\n\nThe engineer also asks: {question.strip()}"
    return text


class AnthropicProvider:
    """Summaries via the Claude API (``pip install queueloom[ai]``)."""

    name = "anthropic"

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        *,
        api_key: str | None = None,
        max_tokens: int = 2000,
        effort: str = "medium",
        fallbacks: bool = True,
        timeout: float = 120.0,
        client: Any = None,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self.effort = effort
        self.fallbacks = fallbacks
        if client is None:
            try:
                import anthropic
            except ImportError as exc:  # pragma: no cover - exercised only without the extra
                raise SummaryError(
                    "The anthropic package is not installed; run `pip install queueloom[ai]`."
                ) from exc
            client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.client = client

    def _request(self, report: str, question: str | None) -> dict[str, Any]:
        return {
            "model": self.model,
            "max_tokens": self.max_tokens,
            # Stable system prompt first so it is cacheable across requests.
            "system": [
                {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [{"role": "user", "content": build_user_message(report, question)}],
            "output_config": {"effort": self.effort},
        }

    def summarize(self, report: str, *, question: str | None = None) -> Summary:
        request = self._request(report, question)
        try:
            if self.fallbacks:
                # Server-side refusal fallback: if the primary model declines, the API re-runs
                # the request on a fallback model inside the same call.
                response = self.client.beta.messages.create(
                    betas=["server-side-fallback-2026-07-01"], fallbacks="default", **request
                )
            else:
                response = self.client.messages.create(**request)
        except Exception as exc:
            raise SummaryError(_describe_error(exc)) from exc

        if getattr(response, "stop_reason", None) == "refusal":
            details = getattr(response, "stop_details", None)
            category = getattr(details, "category", None)
            raise SummaryError(
                "The model declined to summarise this report"
                + (f" (category: {category})." if category else ".")
            )
        text = "".join(
            getattr(block, "text", "")
            for block in getattr(response, "content", [])
            if getattr(block, "type", None) == "text"
        ).strip()
        if not text:
            raise SummaryError("The model returned an empty summary.")
        usage = getattr(response, "usage", None)
        return Summary(
            text=text,
            provider=self.name,
            model=getattr(response, "model", self.model),
            input_tokens=getattr(usage, "input_tokens", None),
            output_tokens=getattr(usage, "output_tokens", None),
        )


def _describe_error(exc: Exception) -> str:
    """Map SDK exceptions to user-facing messages without importing anthropic eagerly."""
    try:
        import anthropic
    except ImportError:  # pragma: no cover
        return f"AI provider error: {type(exc).__name__}: {exc}"
    if isinstance(exc, anthropic.AuthenticationError):
        return "AI provider rejected the API key (check ANTHROPIC_API_KEY)."
    if isinstance(exc, anthropic.NotFoundError):
        return "AI model not found (check QUEUELOOM_AI_MODEL)."
    if isinstance(exc, anthropic.RateLimitError):
        retry_after = exc.response.headers.get("retry-after") if exc.response else None
        return "AI provider rate limit reached" + (
            f"; retry after {retry_after}s." if retry_after else "."
        )
    if isinstance(exc, anthropic.APIStatusError):
        if exc.status_code >= 500:
            return f"AI provider server error ({exc.status_code}); try again shortly."
        return f"AI provider error ({exc.status_code}): {exc.message}"
    if isinstance(exc, anthropic.APIConnectionError):
        return "Could not reach the AI provider (network error)."
    return f"AI provider error: {type(exc).__name__}: {exc}"


def get_provider(settings: Any) -> SummaryProvider:
    """Choose a provider from server settings (``QUEUELOOM_AI_PROVIDER``)."""
    choice = (getattr(settings, "ai_provider", "auto") or "auto").lower()
    api_key = getattr(settings, "anthropic_api_key", None) or os.environ.get("ANTHROPIC_API_KEY")
    if choice == "template" or (choice == "auto" and not api_key):
        return TemplateProvider()
    if choice in ("anthropic", "auto"):
        return AnthropicProvider(
            getattr(settings, "ai_model", DEFAULT_MODEL) or DEFAULT_MODEL,
            api_key=api_key,
            max_tokens=int(getattr(settings, "ai_max_tokens", 2000)),
            effort=getattr(settings, "ai_effort", "medium") or "medium",
            fallbacks=bool(getattr(settings, "ai_fallbacks", True)),
        )
    raise SummaryError(f"unknown AI provider {choice!r}; use auto, anthropic or template")
