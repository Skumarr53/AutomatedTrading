# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# --  Git Hosting  Information ------------------------------------------------
# https://skumarr53.github.io/AutomatedTrading/_build/html/index.html
import os
import sys
from pathlib import Path

# Resolve the absolute path to the project root directory
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

project = 'Automated Trading'
copyright = '2024, Santhosh Kumar'
author = 'Santhosh Kumar'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
        'sphinx.ext.autosummary',
            'sphinx.ext.intersphinx',


]

autosummary_generate = True

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'sklearn': ('https://scikit-learn.org/stable/', None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
autodoc_typehints = 'description'

html_theme = 'sphinx_material'
html_static_path = ['_static']


epub_show_urls = 'footnote'              # Show URLs as footnotes in EPUB
