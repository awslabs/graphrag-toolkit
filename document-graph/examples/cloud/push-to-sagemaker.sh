#!/bin/bash
# push-to-sagemaker.sh — Rebuild wheel + upload to S3 (SageMaker pulls from there)
# Run locally after making changes to document-graph.
# Then in SageMaker terminal: bash ~/SageMaker/document-graph/install.sh
set -e

cd "$(dirname "$0")/../.."
export AWS_PROFILE="${AWS_PROFILE:-nw}"

echo "Building wheel..."
rm -f dist/*.whl
/Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12 -m build --wheel --outdir dist/ 2>&1 | tail -1

echo "Uploading wheel..."
aws s3 cp dist/document_graph-*.whl s3://graphrag-artifacts-705909755305/document-graph-notebooks/wheels/ --region us-east-1

echo "Cleaning S3 notebooks (removing stale files)..."
aws s3 rm s3://graphrag-artifacts-705909755305/document-graph-notebooks/ --recursive --exclude "wheels/*" --region us-east-1

echo "Uploading notebooks..."
aws s3 sync examples/cloud/notebooks/ s3://graphrag-artifacts-705909755305/document-graph-notebooks/ \
  --exclude "__pycache__/*" --exclude "*.pyc" --exclude "wheels/*" --region us-east-1

echo ""
echo "✅ Pushed. In SageMaker run:"
echo "   aws s3 sync s3://graphrag-artifacts-705909755305/document-graph-notebooks/ ~/SageMaker/document-graph/"
echo "   pip install --force-reinstall ~/SageMaker/document-graph/wheels/document_graph-*.whl"
