#!/bin/bash

if [[ "$#" -gt 0 ]]; then
	if [[ "$1" == "--help" ]]; then
	  echo "Usage: install.sh [options]"
		echo ""
		echo "where options include:"
		echo ""
		echo "  --bucket <bucket name>"
		echo "  --region <AWS region>"
	  exit 0
	fi
fi

if [ -e ./.env ]; then
    source ./.env
fi

STACK_SUFFIX=$(date +%s)
GRAPH_NAME="grw-$STACK_SUFFIX"
STACK_DESCRIPTION="graphrag workshop"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --bucket) BUCKET_NAME="$2"; shift ;;
        --region) REGION_NAME="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [[ -z "$BUCKET_NAME" ]]; then
  echo "You must supply a bucket name either via the --bucket parameter or BUCKET_NAME environment variable"
  exit -1
fi

if [[ -z "$REGION_NAME" ]]; then
  echo "You must supply a region name either via the --region parameter or REGION_NAME environment variable"
  exit -1
fi


S3_PREFIX="graphrag-workshop/$GRAPH_NAME"
S3_URL_ROOT="https://$BUCKET_NAME.s3.amazonaws.com/$S3_PREFIX"
S3_URI_ROOT="s3://$BUCKET_NAME/$S3_PREFIX"
GRAPHRAG_WORKSHOP_S3_URI="$S3_URI_ROOT/graphrag-workshop.zip"

rm -rf temp
mkdir temp

pushd temp

mkdir graphrag-workshop

cp -r ./../assets/graphrag-toolkit-workshop.json .
cp -r ./../assets/* graphrag-workshop

pushd graphrag-workshop
zip -r ../graphrag-workshop.zip .  # add files and subdir direct to zip
popd

rm -rf graphrag-workshop

popd

pwd

aws s3 cp temp/ $S3_URI_ROOT --recursive --region "$REGION_NAME"

rm -rf temp

echo ""
echo "----------------------------------------------------"
echo "S3_URL_ROOT: $S3_URL_ROOT"
echo "S3_URI_ROOT: $S3_URI_ROOT"
echo "GRAPHRAG_WORKSHOP_S3_URI: $GRAPHRAG_WORKSHOP_S3_URI"
echo "----------------------------------------------------"
echo ""

aws cloudformation create-stack --stack-name "$GRAPH_NAME-workshop" \
  --template-url "$S3_URL_ROOT/graphrag-toolkit-workshop.json" \
  --parameters \
	ParameterKey=ApplicationId,ParameterValue="$GRAPH_NAME" \
  ParameterKey=ExampleNotebooksURL,ParameterValue="$GRAPHRAG_WORKSHOP_S3_URI" \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags \
    Key=ApplicationName,Value="graphrag workshop" \
    Key=GraphName,Value=$GRAPH_NAME \
  --region $REGION_NAME
