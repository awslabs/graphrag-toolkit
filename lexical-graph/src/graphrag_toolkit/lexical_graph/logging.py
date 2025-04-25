# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.config
from typing import List, Dict, Optional

EXCLUDED_WARNINGS = [
    'Removing unpickleable private attribute'
]

class CustomFormatter(logging.Formatter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def format(self, record):
        saved_name = record.name  
        parts = saved_name.split('.')
        record.name = parts[-1]
        if len(parts) > 1:
            record.name = f"{'.'.join(p[0] for p in parts[0:-1])}.{parts[-1]}"
        result = super().format(record)
        record.name = saved_name
        return result

class ModuleFilter(logging.Filter):
    def __init__(self, info:Dict[str, List[str]]={}, debug:Dict[str, List[str]]={}):
        logging.Filter.__init__(self)
        self.info_include_modules = info.get('include_modules', [])
        self.info_exclude_modules = info.get('exclude_modules', [])
        self.debug_include_modules = debug.get('include_modules', [])
        self.debug_exclude_modules = debug.get('exclude_modules', [])
        
    def filter(self, record):
        if record.levelno == logging.INFO:
            name = record.name
            if any(name.startswith(x) for x in self.info_exclude_modules):
                return False
            if any(name.startswith(x) for x in self.info_include_modules):
                return True
            if '*' in self.info_exclude_modules:
                return False
            if '*' in self.info_include_modules:
                return True
            return record.name.startswith('graphrag_toolkit')
        elif record.levelno == logging.DEBUG:
            name = record.name
            if any(name.startswith(x) for x in self.debug_exclude_modules):
                return False
            if any(name.startswith(x) for x in self.debug_include_modules):
                return True
            if '*' in self.debug_exclude_modules:
                return False
            if '*' in self.debug_include_modules:
                return True
            return False
        elif record.levelno == logging.WARNING:
            message = record.getMessage()
            if any(message.startswith(x) for x in EXCLUDED_WARNINGS):
                return False
        return True

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters' : {
        'moduleFilter' : {
            '()': ModuleFilter,
            'info': {
                'include_modules': [
                    '*'
                ],
                'exclude_modules': [ 
                    'opensearch',
                    'boto'
                ]
            },
            'debug': {
                'include_modules': [ 
                ],
                'exclude_modules': [ 
                    'opensearch',
                    'boto'
                ]
            }            
        }
    },
    'formatters': {
        'default': {
            '()': CustomFormatter,
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
            LOGGING['filters']['moduleFilter']['debug']['include_modules'] = debug_include_modules
            
    if debug_exclude_modules is not None:
            debug_exclude_modules = debug_exclude_modules if isinstance(debug_exclude_modules, list) else [debug_exclude_modules]
            LOGGING['filters']['moduleFilter']['debug']['exclude_modules'] = debug_exclude_modules
    
    logging.config.dictConfig(LOGGING)

