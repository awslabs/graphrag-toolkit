"""Sphinx configuration for document-graph."""

project = 'document-graph'
copyright = '2026, Evan Erwee'
author = 'Evan Erwee'
release = '3.0.4'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns = ['_build']

html_theme = 'furo'
html_static_path = ['_static']

# MyST (markdown support)
source_suffix = {'.rst': 'restructuredtext', '.md': 'markdown'}
myst_enable_extensions = ['colon_fence', 'deflist']
