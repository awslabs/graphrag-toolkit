#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

DO_SETUP=true
REMAINING_ARGS=()

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-setup) DO_SETUP=false ;;
        *) REMAINING_ARGS+=($1) ;;
    esac
    shift
done

echo "REMAINING_ARGS: ${REMAINING_ARGS[@]}"
echo "DO_SETUP:       $DO_SETUP"

declare -p DO_SETUP REMAINING_ARGS > ~/all_vars

sudo -u ec2-user -i <<'EOF'

ENVIRONMENT=JupyterSystemEnv

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

pushd /home/ec2-user/SageMaker/graphrag-toolkit

source ~/all_vars
rm -f ~/all_vars
source ./.env.testing
source ./.env

echo "REMAINING_ARGS: ${REMAINING_ARGS[@]}"
echo "DO_SETUP:       $DO_SETUP"

if [[ "$DO_SETUP" = true ]]; then

    echo "Installing toolkit and dependencies..."
    
    aws s3 cp $GRAPHRAG_TOOLKIT_S3_URI .

    unzip graphrag-toolkit.zip
    
    cp graphrag-toolkit/*.* .
    mv graphrag-toolkit/graphrag_toolkit/ .
    mv graphrag-toolkit/falkordb/ .
    
    rm -rf graphrag-toolkit.zip
    rm -rf graphrag-toolkit

    if [[ "$BYOKG_RAG_INSTALL_URI" ]]; then
        echo "Installing byokg_rag from $BYOKG_RAG_INSTALL_URI"
        if [[ "$BYOKG_RAG_INSTALL_URI" == s3://* && "$BYOKG_RAG_INSTALL_URI" == *.whl ]]; then
            WHEEL_FILENAME=$(basename "$BYOKG_RAG_INSTALL_URI")
            echo "Downloading wheel from S3: $BYOKG_RAG_INSTALL_URI"
            if ! aws s3 cp "$BYOKG_RAG_INSTALL_URI" "./$WHEEL_FILENAME"; then
                echo "ERROR: Failed to download wheel from S3: $BYOKG_RAG_INSTALL_URI"
                exit 1
            fi
            pip install "./$WHEEL_FILENAME"
            rm -f "./$WHEEL_FILENAME"
        else
            pip install $BYOKG_RAG_INSTALL_URI
        fi
    fi

    if [[ "$LEXICAL_GRAPH_INSTALL_URI" ]]; then
        echo "Installing lexical graph from $LEXICAL_GRAPH_INSTALL_URI"
        if [[ "$LEXICAL_GRAPH_INSTALL_URI" == s3://* && "$LEXICAL_GRAPH_INSTALL_URI" == *.whl ]]; then
            WHEEL_FILENAME=$(basename "$LEXICAL_GRAPH_INSTALL_URI")
            echo "Downloading wheel from S3: $LEXICAL_GRAPH_INSTALL_URI"
            if ! aws s3 cp "$LEXICAL_GRAPH_INSTALL_URI" "./$WHEEL_FILENAME"; then
                echo "ERROR: Failed to download wheel from S3: $LEXICAL_GRAPH_INSTALL_URI"
                exit 1
            fi
            pip install "./$WHEEL_FILENAME"
            rm -f "./$WHEEL_FILENAME"
        else
            pip install $LEXICAL_GRAPH_INSTALL_URI
        fi
    fi

    echo "Installing all dependencies in a single pass for consistent resolution"
    grep -v '^--' graphrag_toolkit/byokg_rag/requirements.txt > /tmp/byokg_rag_deps.txt
    grep -v '^--' graphrag_toolkit/lexical_graph/requirements.txt > /tmp/lexical_graph_deps.txt
    pip install \
        -r /tmp/byokg_rag_deps.txt \
        -r /tmp/lexical_graph_deps.txt \
        -r requirements-integ-test.txt

    #if [[ "$USE_GPU" == "True" ]]; then
    #    pip install --upgrade cmake
    #    pip install --extra-index-url https://pypi.fury.io/arrow-nightlies/ --prefer-binary --pre pyarrow
    #    pip install torch FlagEmbedding
    #fi

    pushd falkordb
        pip install .
    popd

    mkdir test-results
    mkdir test-logs

    python -m spacy download en_core_web_sm

    python --version
    pip list
fi

# If BENCHMARK_ALL_RETRIEVERS is set, run extract+build once then loop query+evaluate per retriever
if [[ "$BENCHMARK_ALL_RETRIEVERS" == "true" ]]; then
    bash run_all_retrievers.sh "$BENCHMARK_DATASET"
else
    python test_suite.py "${REMAINING_ARGS[@]}"
fi

popd

conda deactivate

EOF
