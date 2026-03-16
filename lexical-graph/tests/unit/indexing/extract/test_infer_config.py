# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.extract.infer_config import InferClassificationsConfig


class TestInferClassificationsConfigInitialization:
    """Tests for InferClassificationsConfig initialization."""
    
    def test_initialization_with_defaults(self):
        """Verify InferClassificationsConfig initializes with default values."""
        config = InferClassificationsConfig()
        
        assert config.num_samples == 5
        assert config.num_iterations == 1
        assert config.num_classifications == 15
        assert config.prompt_template is None
        assert config.replace_default_classifications is False
    
    def test_initialization_with_custom_values(self):
        """Verify InferClassificationsConfig initializes with custom values."""
        config = InferClassificationsConfig(
            num_samples=10,
            num_iterations=3,
            num_classifications=20,
            prompt_template="Custom prompt template",
            replace_default_classifications=True
        )
        
        assert config.num_samples == 10
        assert config.num_iterations == 3
        assert config.num_classifications == 20
        assert config.prompt_template == "Custom prompt template"
        assert config.replace_default_classifications is True
    
    def test_initialization_with_partial_values(self):
        """Verify InferClassificationsConfig accepts partial custom values."""
        config = InferClassificationsConfig(
            num_samples=15,
            num_iterations=2
        )
        
        assert config.num_samples == 15
        assert config.num_iterations == 2
        assert config.num_classifications == 15  # default
        assert config.prompt_template is None  # default
        assert config.replace_default_classifications is False  # default


class TestInferClassificationsConfigNumSamples:
    """Tests for num_samples configuration."""
    
    def test_num_samples_default(self):
        """Verify default num_samples is 5."""
        config = InferClassificationsConfig()
        assert config.num_samples == 5
    
    def test_num_samples_custom(self):
        """Verify custom num_samples is stored correctly."""
        config = InferClassificationsConfig(num_samples=100)
        assert config.num_samples == 100
    
    def test_num_samples_zero(self):
        """Verify num_samples can be set to zero."""
        config = InferClassificationsConfig(num_samples=0)
        assert config.num_samples == 0
    
    def test_num_samples_large_value(self):
        """Verify num_samples accepts large values."""
        config = InferClassificationsConfig(num_samples=10000)
        assert config.num_samples == 10000


class TestInferClassificationsConfigNumIterations:
    """Tests for num_iterations configuration."""
    
    def test_num_iterations_default(self):
        """Verify default num_iterations is 1."""
        config = InferClassificationsConfig()
        assert config.num_iterations == 1
    
    def test_num_iterations_custom(self):
        """Verify custom num_iterations is stored correctly."""
        config = InferClassificationsConfig(num_iterations=5)
        assert config.num_iterations == 5
    
    def test_num_iterations_multiple(self):
        """Verify num_iterations accepts multiple iterations."""
        config = InferClassificationsConfig(num_iterations=10)
        assert config.num_iterations == 10


class TestInferClassificationsConfigNumClassifications:
    """Tests for num_classifications configuration."""
    
    def test_num_classifications_default(self):
        """Verify default num_classifications is 15."""
        config = InferClassificationsConfig()
        assert config.num_classifications == 15
    
    def test_num_classifications_custom(self):
        """Verify custom num_classifications is stored correctly."""
        config = InferClassificationsConfig(num_classifications=25)
        assert config.num_classifications == 25
    
    def test_num_classifications_small_value(self):
        """Verify num_classifications accepts small values."""
        config = InferClassificationsConfig(num_classifications=3)
        assert config.num_classifications == 3


class TestInferClassificationsConfigPromptTemplate:
    """Tests for prompt_template configuration."""
    
    def test_prompt_template_default(self):
        """Verify default prompt_template is None."""
        config = InferClassificationsConfig()
        assert config.prompt_template is None
    
    def test_prompt_template_custom(self):
        """Verify custom prompt_template is stored correctly."""
        template = "Analyze the following text: {text}"
        config = InferClassificationsConfig(prompt_template=template)
        assert config.prompt_template == template
    
    def test_prompt_template_empty_string(self):
        """Verify prompt_template can be empty string."""
        config = InferClassificationsConfig(prompt_template="")
        assert config.prompt_template == ""
    
    def test_prompt_template_multiline(self):
        """Verify prompt_template handles multiline strings."""
        template = """
        Line 1
        Line 2
        Line 3
        """
        config = InferClassificationsConfig(prompt_template=template)
        assert config.prompt_template == template


class TestInferClassificationsConfigReplaceDefault:
    """Tests for replace_default_classifications configuration."""
    
    def test_replace_default_classifications_default(self):
        """Verify default replace_default_classifications is False."""
        config = InferClassificationsConfig()
        assert config.replace_default_classifications is False
    
    def test_replace_default_classifications_true(self):
        """Verify replace_default_classifications can be set to True."""
        config = InferClassificationsConfig(replace_default_classifications=True)
        assert config.replace_default_classifications is True
    
    def test_replace_default_classifications_false(self):
        """Verify replace_default_classifications can be explicitly set to False."""
        config = InferClassificationsConfig(replace_default_classifications=False)
        assert config.replace_default_classifications is False


class TestInferClassificationsConfigCombinations:
    """Tests for various configuration combinations."""
    
    def test_all_parameters_set(self):
        """Verify all parameters can be set together."""
        config = InferClassificationsConfig(
            num_samples=50,
            num_iterations=5,
            num_classifications=30,
            prompt_template="Custom template",
            replace_default_classifications=True
        )
        
        assert config.num_samples == 50
        assert config.num_iterations == 5
        assert config.num_classifications == 30
        assert config.prompt_template == "Custom template"
        assert config.replace_default_classifications is True
    
    def test_multiple_configs_independent(self):
        """Verify multiple config instances are independent."""
        config1 = InferClassificationsConfig(num_samples=10)
        config2 = InferClassificationsConfig(num_samples=20)
        
        assert config1.num_samples == 10
        assert config2.num_samples == 20
        assert config1.num_samples != config2.num_samples
