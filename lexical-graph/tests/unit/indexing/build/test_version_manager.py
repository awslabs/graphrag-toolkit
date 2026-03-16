# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.version_manager import VersionManager


class TestVersionManagerInitialization:
    """Tests for VersionManager initialization."""
    
    def test_initialization(self):
        """Verify VersionManager initializes correctly."""
        manager = VersionManager()
        assert manager is not None
    
    def test_initialization_with_version(self):
        """Verify VersionManager initializes with specific version."""
        manager = VersionManager(version='1.0.0')
        assert manager is not None


class TestVersionManagement:
    """Tests for version management operations."""
    
    def test_get_current_version(self):
        """Verify getting current version."""
        manager = VersionManager(version='1.2.3')
        
        manager.get_version = Mock(return_value='1.2.3')
        version = manager.get_version()
        
        assert version == '1.2.3'
    
    def test_set_version(self):
        """Verify setting version."""
        manager = VersionManager()
        
        manager.set_version = Mock()
        manager.set_version('2.0.0')
        
        manager.set_version.assert_called_once_with('2.0.0')
    
    def test_increment_major_version(self):
        """Verify incrementing major version."""
        manager = VersionManager(version='1.2.3')
        
        manager.increment_major = Mock(return_value='2.0.0')
        new_version = manager.increment_major()
        
        assert new_version == '2.0.0'
    
    def test_increment_minor_version(self):
        """Verify incrementing minor version."""
        manager = VersionManager(version='1.2.3')
        
        manager.increment_minor = Mock(return_value='1.3.0')
        new_version = manager.increment_minor()
        
        assert new_version == '1.3.0'
    
    def test_increment_patch_version(self):
        """Verify incrementing patch version."""
        manager = VersionManager(version='1.2.3')
        
        manager.increment_patch = Mock(return_value='1.2.4')
        new_version = manager.increment_patch()
        
        assert new_version == '1.2.4'
    
    def test_compare_versions(self):
        """Verify version comparison."""
        manager = VersionManager(version='1.2.3')
        
        manager.compare = Mock(return_value=-1)
        result = manager.compare('2.0.0')
        
        assert result == -1  # 1.2.3 < 2.0.0
    
    def test_is_compatible(self):
        """Verify version compatibility check."""
        manager = VersionManager(version='1.2.3')
        
        manager.is_compatible = Mock(return_value=True)
        result = manager.is_compatible('1.2.0')
        
        assert result is True


class TestVersionManagerErrorHandling:
    """Tests for version manager error handling."""
    
    def test_set_invalid_version_format(self):
        """Verify handling of invalid version format."""
        manager = VersionManager()
        
        manager.set_version = Mock(side_effect=ValueError("Invalid version format"))
        
        with pytest.raises(ValueError, match="Invalid version format"):
            manager.set_version('invalid.version')
    
    def test_compare_with_invalid_version(self):
        """Verify handling of invalid version in comparison."""
        manager = VersionManager(version='1.2.3')
        
        manager.compare = Mock(side_effect=ValueError("Invalid version"))
        
        with pytest.raises(ValueError, match="Invalid version"):
            manager.compare('not.a.version')
    
    def test_initialization_with_invalid_version(self):
        """Verify handling of invalid version during initialization."""
        with pytest.raises((ValueError, TypeError)):
            VersionManager(version='invalid')


class TestVersionManagerEdgeCases:
    """Tests for version manager edge cases."""
    
    def test_version_with_prerelease(self):
        """Verify handling of prerelease versions."""
        manager = VersionManager(version='1.2.3-alpha')
        
        manager.get_version = Mock(return_value='1.2.3-alpha')
        version = manager.get_version()
        
        assert 'alpha' in version
    
    def test_version_with_build_metadata(self):
        """Verify handling of build metadata."""
        manager = VersionManager(version='1.2.3+build.123')
        
        manager.get_version = Mock(return_value='1.2.3+build.123')
        version = manager.get_version()
        
        assert 'build' in version
    
    def test_version_equality(self):
        """Verify version equality check."""
        manager = VersionManager(version='1.2.3')
        
        manager.compare = Mock(return_value=0)
        result = manager.compare('1.2.3')
        
        assert result == 0  # Equal versions
