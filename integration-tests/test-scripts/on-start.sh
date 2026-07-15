#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

sudo -u ec2-user -i <<'EOF'

ENVIRONMENT=JupyterSystemEnv

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

echo "Installing toolkit and dependencies..."

rm -rf graphrag_toolkit

pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.12.0.zip#subdirectory=lexical-graph

echo "Installing all dependencies in a single pass for consistent resolution"
pip install \
    opensearch-py \
    llama-index-vector-stores-opensearch \
    llama-index-readers-web \
    llama-index-readers-file \
    torch \
    sentence_transformers

python -m spacy download en_core_web_sm

source /home/ec2-user/anaconda3/bin/deactivate

EOF
