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

# Ensure boto3 and botocore are from the same release. Other packages
# (e.g. aiobotocore via s3fs) can downgrade botocore without touching boto3,
# leaving an incompatible pair that crashes on import.
BOTO3_VER=$(python -c "import importlib.metadata; print(importlib.metadata.version('boto3'))")
BOTOCORE_VER=$(python -c "import importlib.metadata; print(importlib.metadata.version('botocore'))")
if [[ "$BOTO3_VER" != "$BOTOCORE_VER" ]]; then
    echo "WARNING: boto3==$BOTO3_VER and botocore==$BOTOCORE_VER are mismatched. Reinstalling compatible pair."
    pip install --force-reinstall "boto3==$BOTOCORE_VER" "botocore==$BOTOCORE_VER"
fi

source /home/ec2-user/anaconda3/bin/deactivate

EOF
