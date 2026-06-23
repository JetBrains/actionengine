import actionengine
from opentelemetry.trace import (
    SpanContext,
    TraceFlags,
    NonRecordingSpan,
    set_span_in_context,
)

from .create_memory import create_memory
from .create_memory_schema import CREATE_MEMORY_SCHEMA
from .get_schema import get_schema
from .get_schema_schema import GET_SCHEMA_SCHEMA
from .search import search
from .search_schema import SEARCH_MEMORIES_SCHEMA
from .search_schemas import search_schemas
from .search_schemas_schema import SEARCH_SCHEMAS_SCHEMA


def register_actions(registry: actionengine.ActionRegistry):
    registry.register(
        CREATE_MEMORY_SCHEMA.name, CREATE_MEMORY_SCHEMA, create_memory
    )
    registry.register(GET_SCHEMA_SCHEMA.name, GET_SCHEMA_SCHEMA, get_schema)
    registry.register(
        SEARCH_MEMORIES_SCHEMA.name, SEARCH_MEMORIES_SCHEMA, search
    )
    registry.register(
        SEARCH_SCHEMAS_SCHEMA.name, SEARCH_SCHEMAS_SCHEMA, search_schemas
    )
    return registry


def autofill_api_inputs(
    registry: actionengine.ActionRegistry, api_url: str, authorization: str
):
    for action_name in (
        CREATE_MEMORY_SCHEMA.name,
        GET_SCHEMA_SCHEMA.name,
        SEARCH_MEMORIES_SCHEMA.name,
        SEARCH_SCHEMAS_SCHEMA.name,
    ):
        if not registry.is_registered(action_name):
            continue
        registry.get_schema(action_name)["api_url"].autofill_with([api_url])
        registry.get_schema(action_name)["authorization"].autofill_with(
            [authorization]
        )


def get_otel_span(action: actionengine.Action):
    span_id = None
    trace_id = action.get_header("x-ae-otel-trace-id", decode=False)
    if trace_id is None:
        return None

    span_id = action.get_telemetry_span_id()
    if span_id is None:
        return None

    span_context = SpanContext(
        trace_id=int.from_bytes(trace_id, "big"),
        span_id=int.from_bytes(span_id, "big"),
        is_remote=True,
    )
    return NonRecordingSpan(span_context)


def get_langfuse_span(action: actionengine.Action):
    otel_span = get_otel_span(action)
    if otel_span is None:
        return None

    import langfuse

    return langfuse.LangfuseSpan(
        otel_span=otel_span, langfuse_client=langfuse.get_client()
    )
