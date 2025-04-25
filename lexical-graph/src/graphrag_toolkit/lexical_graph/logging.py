# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.config
from typing import List, Dict, Optional, TypeAlias, Union

LoggingLevel: TypeAlias = int

EXCLUDED_WARNINGS = [
    'Removing unpickleable private attribute'
]

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
        record_module = record.name

        excluded_modules = self._excluded_modules.get(record.levelno, [])
        if any(record_module.startswith(x) for x in excluded_modules) or '*' in excluded_modules:
            return False

        included_modules = self._included_modules.get(record.levelno, [])
        if any(record_module.startswith(x) for x in included_modules) or '*' in included_modules:
            return True

        record_message = record.getMessage()

        excluded_messages = self._excluded_messages.get(record.levelno, [])
        if any(record_message.startswith(x) for x in excluded_messages):
            return False

        included_messages = self._included_messages.get(record.levelno, [])
        if any(record_message.startswith(x) for x in included_messages):
            return True

        return True

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters' : {
        'moduleFilter' : {
            '()': ModuleFilter,
            'included_modules': {
                logging.INFO: '*',
            },
            'excluded_modules': {
                logging.INFO: ['opensearch', 'boto'],
                logging.DEBUG: ['opensearch', 'boto'],
            },
            'excluded_messages': {
                logging.DEBUG: EXCLUDED_WARNINGS,
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
    'loggers': {'': {'handlers': ['stdout'], 'level': 'INFO'}},
}

def set_logging_config(logging_level:str, 
                       debug_include_modules:Optional[List[str]]=['graphrag_toolkit'],
                       debug_exclude_modules:Optional[List[str]]=None):
    LOGGING['loggers']['']['level'] = logging_level.upper()
    
    if debug_include_modules is not None:
            debug_include_modules = debug_include_modules if isinstance(debug_include_modules, list) else [debug_include_modules]
            LOGGING['filters']['moduleFilter']['include_modules'][logging.DEBUG] = debug_include_modules
            
    if debug_exclude_modules is not None:
            debug_exclude_modules = debug_exclude_modules if isinstance(debug_exclude_modules, list) else [debug_exclude_modules]
            LOGGING['filters']['moduleFilter']['exclude_modules'][logging.DEBUG] = debug_exclude_modules
    
    logging.config.dictConfig(LOGGING)

