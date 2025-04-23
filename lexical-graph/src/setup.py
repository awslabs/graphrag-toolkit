import setuptools
import os

# Get the directory where setup.py is located
base_dir = os.path.dirname(os.path.abspath(__file__))

deps = []
# Use absolute path to requirements.txt
req_path = os.path.join(base_dir, 'requirements.txt')
if not os.path.exists(req_path):
    raise FileNotFoundError(f"Could not find {req_path}")
with open(req_path) as f:
    for line in f.readlines():
        if not line.strip():
            continue
        deps.append(line.strip())

setuptools.setup(
    name='graphrag-toolkit-lexical-graph',
    description="Custom version of AWS GraphRAG Toolkit (lexical graph) for internal testing",
    packages=setuptools.find_packages(where="."),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires=">=3.10",
    install_requires=deps,
    version="3.3.0-SNAPSHOT"
)