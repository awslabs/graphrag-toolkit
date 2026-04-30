# Updating cached foundation model responses

The GraphRAG workshop uses cached responses from Amazon Bedrock to avoid throttling from the foundation model provider and thereby speed up the extraction and agentic question-answering exercises. These responses are stored in the `workshop/assets/cache` in this repository, from where they are uploaded to the `~/SageMaker/graphrag-toolkit/cache` directory on a running SageMaker workshop notebook.

The Bedrockk cached responses are tied to the foundation model used for extraction and question-answering. (This model is specified in the workshop's CloudFormation **ModelId** input parameter). If you change the model, the existing cached responses will not be used. The workshop will continue to work, but it will not use the cached responses.

To create a fresh set of cached responses, upload the `regen_cache.sh` and `regen_cache.py` files from this directory into the `~/SageMaker/graphrag-toolkit` directory on a running SageMaker workshop notebook that has been provisioned with the new model. Open a terminal session on the notebook, and then run:

```
cd ~/SageMaker/graphrag-toolkit
sh regen_cache.sh
```

The script first archives the existing cache directory, and then generates a fresh set of cached responses. Once complete, it compresses the new cache directory into a `cache.zip` file, which you can download.
