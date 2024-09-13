"""A JSON formatter that understands what to do with our keyword logger things."""

import json
import logging

from .kw_logger import th_keyvals_from_record


class ThdsJsonFormatter(logging.Formatter):
    def _format_exception_and_trace(self, record: logging.LogRecord):
        # without the following boilerplate, we would not see exceptions or stack traces
        # get formatted as part of the log output at all.
        formatted = ""
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            formatted += "\n" + record.exc_text
        if record.stack_info:
            formatted += "\n" + self.formatStack(record.stack_info)
        return formatted

    def format(self, record: logging.LogRecord) -> str:
        """Format the record as a JSON string."""
        # We're going to use a dictionary to hold the record data, and then convert it to JSON.
        # This is because we want to be able to add arbitrary key-value pairs to the log record
        # and have them show up in the JSON output.
        record_dict = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "msg": record.getMessage(),
        }
        # Add the extra data, if it exists.
        if record.__dict__.get("extra"):
            record_dict.update(record.__dict__["extra"])
        record_dict.update(th_keyvals_from_record(record) or {})

        # Convert the dictionary to a JSON string.
        formatted = json.dumps(record_dict)
        if exc_text := self._format_exception_and_trace(record):
            formatted += exc_text
        return formatted
