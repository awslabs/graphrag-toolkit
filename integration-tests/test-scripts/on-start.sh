#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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

# Pin boto3/botocore/aiobotocore to compatible versions. aiobotocore constrains
# botocore to a narrow range; without pinning, other installs can pull a boto3
# that imports symbols missing from the allowed botocore (e.g. DocumentModifiedShape).
pip install "boto3>=1.40.61,<1.44" "botocore>=1.40.61,<1.44" "aiobotocore>=2.21,<2.27"

source /home/ec2-user/anaconda3/bin/deactivate

EOF
