# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.config
import warnings
from typing import List, Dict, Optional, TypeAlias, Union, cast

LoggingLevel: TypeAlias = int

class CompactFormatter(logging.Formatter):
    def format(self, record:logging.LogRecord) -> str:
        original_record_name = record.name
        record.name = self._shorten_record_name(record.name)
        result = super().format(record)
        record.name = original_record_name
        return result

    @staticmethod
    def _shorten_record_name(name:str) -> str:
        if '.' not in name:
            return name

        parts = name.split('.')
        return f"{'.'.join(p[0] for p in parts[0:-1])}.{parts[-1]}"

class ModuleFilter(logging.Filter):
    def __init__(
        self,
        included_modules:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
        excluded_modules:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
        included_messages:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
        excluded_messages:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
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
    'filters' : {
        'moduleFilter' : {
            '()': ModuleFilter,
            'included_modules': {
                logging.INFO: '*',
                logging.DEBUG: ['graphrag_toolkit'],
                logging.WARNING: '*'
            },
            'excluded_modules': {
                logging.INFO: ['opensearch', 'boto', 'urllib'],
                logging.DEBUG: ['opensearch', 'boto', 'urllib'],
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
        }
    },
    'loggers': {'': {'handlers': ['stdout'], 'level': logging.INFO}},
}

def set_logging_config(
    logging_level:Union[str, LoggingLevel],
    debug_include_modules:Optional[Union[str, List[str]]]=None,
    debug_exclude_modules:Optional[Union[str, List[str]]]=None
) -> None:
    set_advanced_logging_config(
        logging_level,
        included_modules={logging.DEBUG: debug_include_modules} if debug_include_modules is not None else None,
        excluded_modules={logging.DEBUG: debug_exclude_modules} if debug_exclude_modules is not None else None,
    )

def set_advanced_logging_config(
    logging_level:Union[str, LoggingLevel],
    included_modules:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
    excluded_modules:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
    included_messages:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
    excluded_messages:Optional[Dict[LoggingLevel, Union[str, List[str]]]]=None,
) -> None:
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
    logging.config.dictConfig(config)

def _is_valid_logging_level(level: Union[str, LoggingLevel]) -> bool:
    if isinstance(level, int):
        return level in cast(dict[LoggingLevel, str], logging._levelToName)  # type: ignore
    elif isinstance(level, str):
        return level.upper() in cast(dict[str, LoggingLevel], logging._nameToLevel)  # type: ignore
    return False
