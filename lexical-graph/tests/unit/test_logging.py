# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for logging.py module.

This module tests logger initialization, level configuration, message formatting,
output destinations, and structured logging capabilities.
"""

import pytest
import logging
import logging.config
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.logging import (
    CompactFormatter,
    ModuleFilter,
    BASE_LOGGING_CONFIG,
    set_logging_config,
    set_advanced_logging_config,
    _is_valid_logging_level
)


class TestLoggerInitialization:
    """Tests for logger initialization.
    
    Validates: Requirement 11.1 (deterministic behavior with exact assertions)
    """
    
    def test_compact_formatter_initialization(self):
        """Verify CompactFormatter initializes correctly."""
        formatter = CompactFormatter(
            fmt='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        assert isinstance(formatter, logging.Formatter)
        assert formatter._fmt == '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
        assert formatter.datefmt == '%Y-%m-%d %H:%M:%S'
    
    def test_module_filter_initialization_empty(self):
        """Verify ModuleFilter initializes with no filters."""
        filter_obj = ModuleFilter()
        
        assert isinstance(filter_obj, logging.Filter)
        assert filter_obj._included_modules == {}
        assert filter_obj._excluded_modules == {}
        assert filter_obj._included_messages == {}
        assert filter_obj._excluded_messages == {}
    
    def test_module_filter_initialization_with_modules(self):
        """Verify ModuleFilter initializes with module filters."""
        filter_obj = ModuleFilter(
            included_modules={logging.INFO: ['graphrag_toolkit']},
            excluded_modules={logging.DEBUG: ['boto', 'urllib']}
        )
        
        assert filter_obj._included_modules[logging.INFO] == ['graphrag_toolkit']
        assert filter_obj._excluded_modules[logging.DEBUG] == ['boto', 'urllib']
    
    def test_module_filter_initialization_with_string_modules(self):
        """Verify ModuleFilter converts string modules to lists."""
        filter_obj = ModuleFilter(
            included_modules={logging.INFO: 'graphrag_toolkit'},
            excluded_modules={logging.DEBUG: 'boto'}
        )
        
        assert filter_obj._included_modules[logging.INFO] == ['graphrag_toolkit']
        assert filter_obj._excluded_modules[logging.DEBUG] == ['boto']
    
    def test_module_filter_initialization_with_messages(self):
        """Verify ModuleFilter initializes with message filters."""
        filter_obj = ModuleFilter(
            included_messages={logging.INFO: ['Starting']},
            excluded_messages={logging.WARNING: ['Removing unpickleable']}
        )
        
        assert filter_obj._included_messages[logging.INFO] == ['Starting']
        assert filter_obj._excluded_messages[logging.WARNING] == ['Removing unpickleable']
    
    def test_base_logging_config_structure(self):
        """Verify BASE_LOGGING_CONFIG has correct structure."""
        assert BASE_LOGGING_CONFIG['version'] == 1
        assert BASE_LOGGING_CONFIG['disable_existing_loggers'] is False
        assert 'filters' in BASE_LOGGING_CONFIG
        assert 'formatters' in BASE_LOGGING_CONFIG
        assert 'handlers' in BASE_LOGGING_CONFIG
        assert 'loggers' in BASE_LOGGING_CONFIG


class TestLoggerLevelConfiguration:
    """Tests for logger level configuration.
    
    Validates: Requirement 11.1 (deterministic behavior with exact assertions)
    """
    
    def test_is_valid_logging_level_string_valid(self):
        """Verify _is_valid_logging_level accepts valid string levels."""
        assert _is_valid_logging_level('DEBUG') is True
        assert _is_valid_logging_level('INFO') is True
        assert _is_valid_logging_level('WARNING') is True
        assert _is_valid_logging_level('ERROR') is True
        assert _is_valid_logging_level('CRITICAL') is True
    
    def test_is_valid_logging_level_string_case_insensitive(self):
        """Verify _is_valid_logging_level handles lowercase strings."""
        assert _is_valid_logging_level('debug') is True
        assert _is_valid_logging_level('info') is True
        assert _is_valid_logging_level('warning') is True
    
    def test_is_valid_logging_level_int_valid(self):
        """Verify _is_valid_logging_level accepts valid integer levels."""
        assert _is_valid_logging_level(logging.DEBUG) is True
        assert _is_valid_logging_level(logging.INFO) is True
        assert _is_valid_logging_level(logging.WARNING) is True
        assert _is_valid_logging_level(logging.ERROR) is True
        assert _is_valid_logging_level(logging.CRITICAL) is True
    
    def test_is_valid_logging_level_invalid_string(self):
        """Verify _is_valid_logging_level rejects invalid strings."""
        assert _is_valid_logging_level('INVALID') is False
        assert _is_valid_logging_level('TRACE') is False
        assert _is_valid_logging_level('') is False
    
    def test_is_valid_logging_level_invalid_int(self):
        """Verify _is_valid_logging_level rejects invalid integers."""
        assert _is_valid_logging_level(999) is False
        assert _is_valid_logging_level(-1) is False
    
    def test_is_valid_logging_level_invalid_type(self):
        """Verify _is_valid_logging_level rejects invalid types."""
        assert _is_valid_logging_level(None) is False
        assert _is_valid_logging_level([]) is False
        assert _is_valid_logging_level({}) is False
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_debug_level(self, mock_dict_config):
        """Verify set_logging_config sets DEBUG level."""
        set_logging_config('DEBUG')
        
        mock_dict_config.assert_called_once()
        config = mock_dict_config.call_args[0][0]
        assert config['loggers']['']['level'] == 'DEBUG'
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_info_level(self, mock_dict_config):
        """Verify set_logging_config sets INFO level."""
        set_logging_config('INFO')
        
        mock_dict_config.assert_called_once()
        config = mock_dict_config.call_args[0][0]
        assert config['loggers']['']['level'] == 'INFO'
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_int_level(self, mock_dict_config):
        """Verify set_logging_config accepts integer levels."""
        set_logging_config(logging.WARNING)
        
        mock_dict_config.assert_called_once()
        config = mock_dict_config.call_args[0][0]
        assert config['loggers']['']['level'] == 'WARNING'
    
    @patch('logging.config.dictConfig')
    @patch('warnings.warn')
    def test_set_logging_config_invalid_level_warns(self, mock_warn, mock_dict_config):
        """Verify set_logging_config warns on invalid level."""
        set_logging_config('INVALID')
        
        mock_warn.assert_called_once()
        assert 'Unknown logging level' in str(mock_warn.call_args[0][0])


class TestLogMessageFormatting:
    """Tests for log message formatting.
    
    Validates: Requirement 11.1 (deterministic behavior with exact assertions)
    """
    
    def test_compact_formatter_shorten_record_name_no_dots(self):
        """Verify _shorten_record_name returns unchanged name without dots."""
        result = CompactFormatter._shorten_record_name('simple')
        assert result == 'simple'
    
    def test_compact_formatter_shorten_record_name_one_dot(self):
        """Verify _shorten_record_name shortens name with one dot."""
        result = CompactFormatter._shorten_record_name('graphrag.toolkit')
        assert result == 'g.toolkit'
    
    def test_compact_formatter_shorten_record_name_multiple_dots(self):
        """Verify _shorten_record_name shortens deeply nested names."""
        result = CompactFormatter._shorten_record_name('graphrag_toolkit.lexical_graph.indexing.build')
        assert result == 'g.l.i.build'
    
    def test_compact_formatter_shorten_record_name_preserves_last_part(self):
        """Verify _shorten_record_name always preserves the last part."""
        result = CompactFormatter._shorten_record_name('a.b.c.d.e.final_module')
        assert result.endswith('.final_module')
        assert result == 'a.b.c.d.e.final_module'
    
    def test_compact_formatter_format_shortens_name(self):
        """Verify CompactFormatter.format shortens record name."""
        formatter = CompactFormatter(fmt='%(name)s:%(message)s')
        record = logging.LogRecord(
            name='graphrag_toolkit.lexical_graph.indexing',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        
        # Name should be shortened in output
        assert 'g.l.indexing' in result
        assert 'Test message' in result
    
    def test_compact_formatter_format_restores_original_name(self):
        """Verify CompactFormatter.format restores original record name."""
        formatter = CompactFormatter(fmt='%(name)s:%(message)s')
        original_name = 'graphrag_toolkit.lexical_graph.indexing'
        record = logging.LogRecord(
            name=original_name,
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        formatter.format(record)
        
        # Original name should be restored
        assert record.name == original_name
    
    def test_compact_formatter_format_with_timestamp(self):
        """Verify CompactFormatter formats with timestamp."""
        formatter = CompactFormatter(
            fmt='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        record = logging.LogRecord(
            name='test.module',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        
        # Should contain timestamp, level, shortened name, and message
        assert 'INFO' in result
        assert 't.module' in result
        assert 'Test message' in result


class TestLogOutputDestinations:
    """Tests for log output destinations.
    
    Validates: Requirement 11.1 (deterministic behavior with exact assertions)
    """
    
    def test_base_logging_config_has_stdout_handler(self):
        """Verify BASE_LOGGING_CONFIG includes stdout handler."""
        assert 'stdout' in BASE_LOGGING_CONFIG['handlers']
        stdout_handler = BASE_LOGGING_CONFIG['handlers']['stdout']
        assert stdout_handler['class'] == 'logging.StreamHandler'
        assert stdout_handler['stream'] == 'ext://sys.stdout'
    
    def test_base_logging_config_has_file_handler(self):
        """Verify BASE_LOGGING_CONFIG includes file handler."""
        assert 'file_handler' in BASE_LOGGING_CONFIG['handlers']
        file_handler = BASE_LOGGING_CONFIG['handlers']['file_handler']
        assert file_handler['class'] == 'logging.FileHandler'
        assert 'filename' in file_handler
    
    def test_base_logging_config_default_handlers(self):
        """Verify BASE_LOGGING_CONFIG uses stdout handler by default."""
        assert BASE_LOGGING_CONFIG['loggers']['']['handlers'] == ['stdout']
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_without_file(self, mock_dict_config):
        """Verify set_logging_config without filename uses only stdout."""
        set_logging_config('INFO')
        
        config = mock_dict_config.call_args[0][0]
        assert 'stdout' in config['loggers']['']['handlers']
        assert 'file_handler' not in config['loggers']['']['handlers']
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_with_file(self, mock_dict_config):
        """Verify set_logging_config with filename adds file handler."""
        set_logging_config('INFO', filename='test.log')
        
        config = mock_dict_config.call_args[0][0]
        assert 'stdout' in config['loggers']['']['handlers']
        assert 'file_handler' in config['loggers']['']['handlers']
        assert config['handlers']['file_handler']['filename'] == 'test.log'
    
    @patch('logging.config.dictConfig')
    def test_set_advanced_logging_config_with_custom_filename(self, mock_dict_config):
        """Verify set_advanced_logging_config sets custom filename."""
        set_advanced_logging_config('DEBUG', filename='custom.log')
        
        config = mock_dict_config.call_args[0][0]
        assert config['handlers']['file_handler']['filename'] == 'custom.log'
    
    @patch('logging.config.dictConfig')
    def test_set_advanced_logging_config_file_handler_mode(self, mock_dict_config):
        """Verify file handler uses append mode."""
        set_advanced_logging_config('INFO', filename='test.log')
        
        config = mock_dict_config.call_args[0][0]
        assert config['handlers']['file_handler']['mode'] == 'a'


class TestStructuredLogging:
    """Tests for structured logging with filters.
    
    Validates: Requirement 11.1 (deterministic behavior with exact assertions)
    """
    
    def test_module_filter_filter_excluded_message(self):
        """Verify ModuleFilter excludes messages by prefix."""
        filter_obj = ModuleFilter(
            excluded_messages={logging.WARNING: ['Removing unpickleable']}
        )
        record = logging.LogRecord(
            name='test.module',
            level=logging.WARNING,
            pathname='test.py',
            lineno=1,
            msg='Removing unpickleable private attribute',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is False
    
    def test_module_filter_filter_included_message(self):
        """Verify ModuleFilter includes messages by prefix."""
        filter_obj = ModuleFilter(
            included_messages={logging.INFO: ['Starting']}
        )
        record = logging.LogRecord(
            name='test.module',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Starting process',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is True
    
    def test_module_filter_filter_excluded_module(self):
        """Verify ModuleFilter excludes modules by prefix."""
        filter_obj = ModuleFilter(
            excluded_modules={logging.INFO: ['boto', 'urllib']}
        )
        record = logging.LogRecord(
            name='boto.client',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is False
    
    def test_module_filter_filter_included_module(self):
        """Verify ModuleFilter includes modules by prefix."""
        filter_obj = ModuleFilter(
            included_modules={logging.INFO: ['graphrag_toolkit']}
        )
        record = logging.LogRecord(
            name='graphrag_toolkit.lexical_graph',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is True
    
    def test_module_filter_filter_wildcard_included_modules(self):
        """Verify ModuleFilter wildcard includes all modules."""
        filter_obj = ModuleFilter(
            included_modules={logging.INFO: '*'}
        )
        record = logging.LogRecord(
            name='any.module.name',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is True
    
    def test_module_filter_filter_wildcard_excluded_modules(self):
        """Verify ModuleFilter wildcard excludes all modules."""
        filter_obj = ModuleFilter(
            excluded_modules={logging.DEBUG: '*'}
        )
        record = logging.LogRecord(
            name='any.module.name',
            level=logging.DEBUG,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        assert result is False
    
    def test_module_filter_filter_message_priority_over_module(self):
        """Verify ModuleFilter checks messages before modules."""
        filter_obj = ModuleFilter(
            excluded_messages={logging.INFO: ['Excluded']},
            included_modules={logging.INFO: ['test']}
        )
        record = logging.LogRecord(
            name='test.module',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Excluded message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        # Message exclusion should take priority
        assert result is False
    
    def test_module_filter_filter_included_message_priority(self):
        """Verify ModuleFilter included messages take priority."""
        filter_obj = ModuleFilter(
            included_messages={logging.INFO: ['Important']},
            excluded_modules={logging.INFO: ['test']}
        )
        record = logging.LogRecord(
            name='test.module',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Important message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        # Included message should override excluded module
        assert result is True
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_with_debug_include_modules(self, mock_dict_config):
        """Verify set_logging_config sets debug include modules."""
        set_logging_config('DEBUG', debug_include_modules=['graphrag_toolkit'])
        
        config = mock_dict_config.call_args[0][0]
        filter_config = config['filters']['moduleFilter']
        assert logging.DEBUG in filter_config['included_modules']
        assert filter_config['included_modules'][logging.DEBUG] == ['graphrag_toolkit']
    
    @patch('logging.config.dictConfig')
    def test_set_logging_config_with_debug_exclude_modules(self, mock_dict_config):
        """Verify set_logging_config sets debug exclude modules."""
        set_logging_config('DEBUG', debug_exclude_modules=['boto', 'urllib'])
        
        config = mock_dict_config.call_args[0][0]
        filter_config = config['filters']['moduleFilter']
        assert logging.DEBUG in filter_config['excluded_modules']
        assert filter_config['excluded_modules'][logging.DEBUG] == ['boto', 'urllib']
    
    @patch('logging.config.dictConfig')
    def test_set_advanced_logging_config_with_multiple_levels(self, mock_dict_config):
        """Verify set_advanced_logging_config handles multiple log levels."""
        set_advanced_logging_config(
            'INFO',
            included_modules={
                logging.INFO: ['graphrag_toolkit'],
                logging.DEBUG: ['graphrag_toolkit.lexical_graph']
            },
            excluded_modules={
                logging.INFO: ['boto'],
                logging.WARNING: ['urllib']
            }
        )
        
        config = mock_dict_config.call_args[0][0]
        filter_config = config['filters']['moduleFilter']
        
        assert filter_config['included_modules'][logging.INFO] == ['graphrag_toolkit']
        assert filter_config['included_modules'][logging.DEBUG] == ['graphrag_toolkit.lexical_graph']
        assert filter_config['excluded_modules'][logging.INFO] == ['boto']
        assert filter_config['excluded_modules'][logging.WARNING] == ['urllib']
    
    @patch('logging.config.dictConfig')
    def test_set_advanced_logging_config_with_message_filters(self, mock_dict_config):
        """Verify set_advanced_logging_config sets message filters."""
        set_advanced_logging_config(
            'INFO',
            included_messages={logging.INFO: ['Starting', 'Completed']},
            excluded_messages={logging.WARNING: ['Removing unpickleable']}
        )
        
        config = mock_dict_config.call_args[0][0]
        filter_config = config['filters']['moduleFilter']
        
        assert filter_config['included_messages'][logging.INFO] == ['Starting', 'Completed']
        assert filter_config['excluded_messages'][logging.WARNING] == ['Removing unpickleable']
    
    def test_module_filter_filter_level_specific_filtering(self):
        """Verify ModuleFilter applies filters only to specified levels."""
        filter_obj = ModuleFilter(
            excluded_modules={logging.INFO: ['boto']}
        )
        
        # INFO level should be filtered
        info_record = logging.LogRecord(
            name='boto.client',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        assert filter_obj.filter(info_record) is False
        
        # WARNING level should not be filtered (no rule for WARNING)
        warning_record = logging.LogRecord(
            name='boto.client',
            level=logging.WARNING,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        assert filter_obj.filter(warning_record) is False  # No included modules, so False
    
    def test_module_filter_filter_no_matching_rules(self):
        """Verify ModuleFilter returns False when no rules match."""
        filter_obj = ModuleFilter(
            included_modules={logging.INFO: ['graphrag_toolkit']}
        )
        record = logging.LogRecord(
            name='other.module',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = filter_obj.filter(record)
        
        # No matching include rule, should return False
        assert result is False
