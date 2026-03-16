# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.config
import warnings
from typing import List, Dict, Optional, TypeAlias, Union, cast

LoggingLevel: TypeAlias = int


class CompactFormatter(logging.Formatter):
    """Custom logging formatter for compact record name presentation.

    The `CompactFormatter` class provides a custom formatter for Python's logging
    module. Its primary purpose is to shorten the logger's record name for better
    readability in log outputs. This can be useful in applications or services that
    use deeply nested logger names, where a compact format enhances log legibility.

    Attributes:
        None
    """
    def format(self, record: logging.LogRecord) -> str:
        """
        Formats a log record by temporarily shortening its `name` attribute, producing
        a formatted log message based on the shortened name, and then restoring the
        original name of the log record. This is useful for customizing the format of
        log messages by altering the record's `name` attribute.

        Args:
            record (logging.LogRecord): The log record to be formatted. This object
                contains metadata and information about the log event being processed.

        Returns:
            str: The formatted log message derived from the given log record.
        """
        original_record_name = record.name
        record.name = self._shorten_record_name(record.name)
        result = super().format(record)
        record.name = original_record_name
        return result

    @staticmethod
    def _shorten_record_name(name: str) -> str:
        """
        Shortens a fully qualified record name by reducing all parts except the last one to their initials.

        This method takes a dot-separated name and abbreviates each part except the last one,
        keeping the first character of each intermediate part.

        Args:
            name (str): The fully qualified record name to be shortened.

        Returns:
            str: The shortened version of the record name, with intermediate parts
            abbreviated to their initials.
        """
        if '.' not in name:
            return name

        parts = name.split('.')
        return f"{'.'.join(p[0] for p in parts[0:-1])}.{parts[-1]}"


class ModuleFilter(logging.Filter):
    """
    A logging.Filter subclass for filtering log records based on module names or
    messages.

    This class allows selective inclusion or exclusion of log records based on
    their module name or message content. Users can specify rules for filtering
    both modules and messages, with separate configurations for each logging level.

    Attributes:
        _included_modules (dict[LoggingLevel, list[str]]): A mapping of logging
            levels to the list of module names that should be explicitly included
            in logging. A single entry or wildcard (*) can also be specified.
        _excluded_modules (dict[LoggingLevel, list[str]]): A mapping of logging
            levels to the list of module names that should be explicitly excluded
            from logging. A single entry or wildcard (*) can also be specified.
        _included_messages (dict[LoggingLevel, list[str]]): A mapping of logging
            levels to the list of message prefixes that should be explicitly
            included in logging. A single entry or wildcard (*) can also be
            specified.
        _excluded_messages (dict[LoggingLevel, list[str]]): A mapping of logging
            levels to the list of message prefixes that should be explicitly
            excluded from logging. A single entry or wildcard (*) can also be
            specified.
    """
    def __init__(
            self,
            included_modules: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
            excluded_modules: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
            included_messages: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
            excluded_messages: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
    ) -> None:
        super().__init__()
        self._included_modules: dict[LoggingLevel, list[str]] = {
            l: v if isinstance(v, list) else [v] for l, v in (included_modules or {}).items()
        }
        self._excluded_modules: dict[LoggingLevel, list[str]] = {
            l: v if isinstance(v, list) else [v] for l, v in (excluded_modules or {}).items()
        }
        self._included_messages: dict[LoggingLevel, list[str]] = {
            l: v if isinstance(v, list) else [v] for l, v in (included_messages or {}).items()
        }
        self._excluded_messages: dict[LoggingLevel, list[str]] = {
            l: v if isinstance(v, list) else [v] for l, v in (excluded_messages or {}).items()
        }

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filters log records based on message and module inclusion or exclusion rules. This method
        evaluates conditions such as whether a log message or module matches specified patterns,
        and accordingly determines if the log record should be retained or filtered out.

        Args:
            record: The log record to be filtered, containing details such as log message,
                log level, and originating module.

        Returns:
            bool: True if the log record passes the filtering criteria and should be retained;
                False otherwise.
        """
        record_message = record.getMessage()

        excluded_messages = self._excluded_messages.get(record.levelno, [])
        if any(record_message.startswith(x) for x in excluded_messages):
            return False

        included_messages = self._included_messages.get(record.levelno, [])
        if any(record_message.startswith(x) for x in included_messages) or '*' in included_messages:
            return True

        record_module = record.name

        excluded_modules = self._excluded_modules.get(record.levelno, [])
        if any(record_module.startswith(x) for x in excluded_modules) or '*' in excluded_modules:
            return False

        included_modules = self._included_modules.get(record.levelno, [])
        if any(record_module.startswith(x) for x in included_modules) or '*' in included_modules:
            return True

        return False


BASE_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'moduleFilter': {
            '()': ModuleFilter,
            'included_modules': {
                logging.INFO: '*',
                logging.DEBUG: ['graphrag_toolkit'],
                logging.WARNING: '*',
                logging.ERROR: '*'
            },
            'excluded_modules': {
                logging.INFO: ['opensearch', 'boto', 'urllib'],
                logging.DEBUG: ['opensearch', 'boto', 'urllib'],
                logging.WARNING: ['urllib'],
            },
            'excluded_messages': {
                logging.WARNING: ['Removing unpickleable private attribute'],
            },
            'included_messages': {
            }
        }
    },
    'formatters': {
        'default': {
            '()': CompactFormatter,
            'fmt': '%(asctime)s:%(levelname)s:%(name)-15s:%(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'filters': ['moduleFilter'],
            'formatter': 'default'
        },
        'file_handler': {
            'formatter': 'default',
            'class': 'logging.FileHandler',
            'filename': 'output.log',
            'filters': ['moduleFilter'],
            'mode': 'a',
        }
    },
    'loggers': {'': {'handlers': ['stdout'], 'level': logging.INFO}},
}


def set_logging_config(
        logging_level: Union[str, LoggingLevel],
        debug_include_modules: Optional[Union[str, List[str]]] = None,
        debug_exclude_modules: Optional[Union[str, List[str]]] = None,
        filename: Optional[str] = None
) -> None:
    """Sets the logging configuration for the application.

    This function configures the logging behavior based on the provided logging
    level, specific modules to include or exclude for debugging, and an optional
    log file. It is a utility function that delegates to a more advanced logging
    configuration handler.

    Args:
        logging_level: Specifies the logging verbosity level, which can be a string
            representation of a level (e.g., 'DEBUG', 'INFO') or an enumeration
            value of the `LoggingLevel` type.
        debug_include_modules: Defines specific modules to include for debugging.
            Accepts a single module name as a string or a list of module names.
            Optional, defaults to None if not provided.
        debug_exclude_modules: Specifies modules to exclude from debugging. Users
            may provide a string representing a single module name or a list of
            module names. Optional, defaults to None if not provided.
        filename: The name of the file to which logs should be written. If None,
            logs will not be written to a file. Optional, defaults to None.
    """
    set_advanced_logging_config(
        logging_level,
        included_modules={logging.DEBUG: debug_include_modules} if debug_include_modules is not None else None,
        excluded_modules={logging.DEBUG: debug_exclude_modules} if debug_exclude_modules is not None else None,
        filename=filename
    )


def set_advanced_logging_config(
        logging_level: Union[str, LoggingLevel],
        included_modules: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
        excluded_modules: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
        included_messages: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
        excluded_messages: Optional[Dict[LoggingLevel, Union[str, List[str]]]] = None,
        filename: Optional[str] = None
) -> None:
    """
    Configures advanced logging options to fine-tune logging behavior. Allows setting
    logging levels, filtering log messages by modules or content, and specifying
    a log file for output. This function updates a base logging configuration with
    additional parameters as provided.

    Args:
        logging_level (Union[str, LoggingLevel]): The logging level to be applied
            (e.g., "DEBUG", "INFO", etc.). Supports both logging level names and
            integer level values.
        included_modules (Optional[Dict[LoggingLevel, Union[str, List[str]]]]):
            Dictionaries to specify modules that should be included in logging
            for specific logging levels.
        excluded_modules (Optional[Dict[LoggingLevel, Union[str, List[str]]]]):
            Dictionaries to specify modules that should be excluded from logging
            for specific logging levels.
        included_messages (Optional[Dict[LoggingLevel, Union[str, List[str]]]]):
            Dictionaries to define messages or message patterns to include in
            logging for certain logging levels.
        excluded_messages (Optional[Dict[LoggingLevel, Union[str, List[str]]]]):
            Dictionaries to define messages or message patterns to exclude from
            logging for specific logging levels.
        filename (Optional[str]): Path to the file where logs should be written,
            enabling file-based logging. If not provided, logging output remains
            console-based.

    """
    from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
    
    if not _is_valid_logging_level(logging_level):
        warnings.warn(f'Unknown logging level {logging_level!r} provided.', UserWarning)
    if isinstance(logging_level, int):
        logging_level = logging.getLevelName(logging_level)

    config = BASE_LOGGING_CONFIG.copy()
    config['loggers']['']['level'] = logging_level.upper()
    config['filters']['moduleFilter']['included_modules'].update(included_modules or dict())
    config['filters']['moduleFilter']['excluded_modules'].update(excluded_modules or dict())
    config['filters']['moduleFilter']['included_messages'].update(included_messages or dict())
    config['filters']['moduleFilter']['excluded_messages'].update(excluded_messages or dict())
    
    if filename:
        import os
        if GraphRAGConfig.log_output_dir and not os.path.isabs(filename):
            filename = os.path.join(GraphRAGConfig.log_output_dir, filename)
        config['handlers']['file_handler']['filename'] = filename
        config['loggers']['']['handlers'].append('file_handler')
    
    logging.config.dictConfig(config)


def _is_valid_logging_level(level: Union[str, LoggingLevel]) -> bool:
    """
    Checks if the provided logging level is valid.

    This function verifies whether the given logging level, provided either as a string or
    an integer, corresponds to a recognized logging level in the Python logging module.

    Args:
        level: The logging level to validate, which can be either a string (e.g., "INFO")
            or an integer (e.g., 10 corresponding to logging.DEBUG).

    Returns:
        bool: True if the logging level is valid, False otherwise.
    """
    if isinstance(level, int):
        return level in cast(dict[LoggingLevel, str], logging._levelToName)  # type: ignore
    elif isinstance(level, str):
        return level.upper() in cast(dict[str, LoggingLevel], logging._nameToLevel)  # type: ignore
    return False
