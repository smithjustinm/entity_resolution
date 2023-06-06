"""Logging processors for structlog."""

from typing import Any, Dict

import structlog


def edit_event_name(_, __, event_dict: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Edit the event dict to change the event name, so we don't clobber elastic indices.

    Args:
        _: Unused
        __: Unused
        event_dict: The logging event dictionary

    Returns:
        The modified logging event dictionary
    """
    event = event_dict.pop("event")
    event_dict["message"] = event
    return event_dict


STD_LIB_PROCESSORS = [
    edit_event_name,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


STRUCTLOG_PROCESSORS = [
    edit_event_name,
    structlog.contextvars.merge_contextvars,
    # Add a timestamp in ISO 8601 format.
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.stdlib.add_log_level,
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
    structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
]
