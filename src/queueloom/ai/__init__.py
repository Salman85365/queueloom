"""AI incident summaries.

The model never sees raw telemetry. It receives the deterministic report produced by
:mod:`queueloom.server.diagnosis` and is asked to explain it, rank likely causes and suggest
next steps. Providers sit behind :class:`SummaryProvider` so the LLM vendor is replaceable;
:class:`TemplateProvider` returns the report itself and is used when no model is configured.
"""

from queueloom.ai.providers import (
    AnthropicProvider,
    Summary,
    SummaryError,
    SummaryProvider,
    TemplateProvider,
    get_provider,
)

__all__ = [
    "AnthropicProvider",
    "Summary",
    "SummaryError",
    "SummaryProvider",
    "TemplateProvider",
    "get_provider",
]
