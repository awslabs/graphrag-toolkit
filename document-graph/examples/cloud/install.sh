#!/bin/bash
# Lifecycle config — must finish in <5 min. Do absolute minimum.
set -e
aws s3 sync s3://graphrag-artifacts-705909755305/document-graph-notebooks/ ~/SageMaker/document-graph/ --quiet --exclude "*.ipynb_checkpoints/*"
echo "done" > ~/SageMaker/document-graph/.ready
