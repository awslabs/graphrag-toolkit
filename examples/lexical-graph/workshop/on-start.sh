#!/bin/bash

sudo -u ec2-user -i <<'EOF'

ENVIRONMENT=JupyterSystemEnv

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

echo "Installing toolkit and dependencies..."

#pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.12.0.zip#subdirectory=lexical-graph
pip install -r /home/ec2-user/SageMaker/graphrag-toolkit/graphrag_toolkit/lexical_graph/requirements.txt

pip install opensearch-py==2.8.0 llama-index-vector-stores-opensearch==0.6.2

pip install llama-index-readers-web==0.5.5
pip install llama-index-readers-file==0.5.4
pip install fastmcp==2.12.5 strands-agents==1.13.0

pip install torch==2.6.0 sentence_transformers==5.1.1

python -m spacy download en_core_web_sm

source /home/ec2-user/SageMaker/graphrag-toolkit/.env
python /home/ec2-user/SageMaker/graphrag-toolkit/setup.py

mv /home/ec2-user/SageMaker/graphrag-toolkit/setup.py /home/ec2-user/SageMaker/graphrag-toolkit/.setup.py

source /home/ec2-user/anaconda3/bin/deactivate

EOF
