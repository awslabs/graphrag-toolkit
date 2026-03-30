# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#!/bin/bash

sudo -u ec2-user -i <<'EOF'

ENVIRONMENT=JupyterSystemEnv

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

echo "Installing toolkit and dependencies..."

rm -rf graphrag_toolkit

pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.12.0.zip#subdirectory=lexical-graph

pip install opensearch-py llama-index-vector-stores-opensearch

pip install llama-index-readers-web
pip install llama-index-readers-file

pip install torch sentence_transformers
#pip install numpy==1.26.1

python -m spacy download en_core_web_sm

source /home/ec2-user/anaconda3/bin/deactivate

EOF
