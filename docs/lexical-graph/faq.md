[[Home](./)]

## FAQ

  - [Errors and warnings](#errors-and-warnings)
    - [ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: \<identity\> is not authorized to perform: bedrock:InvokeModel](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-identity-is-not-authorized-to-perform-bedrockinvokemodel)
    - [ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-you-dont-have-access-to-the-model-with-the-specified-model-id)
    - [WARNING:graph_store:Retrying query in x seconds because it raised ConcurrentModificationException](#warninggraph_storeretrying-query-in-x-seconds-because-it-raised-concurrentmodificationexception)

### Errors and warnings

#### ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: \<identity\> is not authorized to perform: bedrock:InvokeModel

If the AWS Identity and Access Management (IAM) identity under which your application is running does not have permission to invoke an Amazon Bedrock foundation model, you will get an error similar to the following:

```
graphrag_toolkit.errors.ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: <identity> is not authorized to perform: bedrock:InvokeModel on resource: arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0 because no identity-based policy allows the bedrock:InvokeModel action [Model config: {"system_prompt": null, "pydantic_program_mode": "default", "model": "anthropic.claude-3-5-haiku-20241022-v1:0", "temperature": 0.0, "max_tokens": 4096, "context_size": 200000, "profile_name": null, "max_retries": 10, "timeout": 60.0, "additional_kwargs": {}, "class_name": "Bedrock_LLM"}]
```

To fix, ensure you have [enabled access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to the appropriate foundation models in Amazon Bedrock, and then update the IAM policy associated with the identity:

```
{
    "Effect": "Allow",
    "Action": [
        "bedrock:InvokeModel"
    ],
    "Resource": [
        "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0"
    ]
}
```

---

#### ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID

Access to Amazon Bedrock foundation models isn't granted by default. If you have not enabled access to a foundation model, you will get an error similar to the following:

```
graphrag_toolkit.errors.ModelError: An error occurred (AccessDeniedException) when calling the InvokeModel operation: You don't have access to the model with the specified model ID. [Model config: {"system_prompt": null, "pydantic_program_mode":"default", "model": "anthropic.claude-3-7-sonnet-20250219-v1:0", "temperature": 0.0, "max_tokens": 4096, "context_size": 200000, "profile_name": null, "max_retries": 10, "timeout": 60.0, "additional_kwargs": {}, "class_name": "Bedrock_LLM"}]
```

To fix,  [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to the appropriate foundation models in Amazon Bedrock, and then [grant IAM permissions to the model](#modelerror-an-error-occurred-accessdeniedexception-when-calling-the-invokemodel-operation-identity-is-not-authorized-to-perform-bedrockinvokemodel).

---

#### Importing the package patches llama_index async internals

When you import `graphrag_toolkit.lexical_graph`, the package patches `llama_index.core.async_utils.asyncio_run` unconditionally ([`__init__.py:34`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/__init__.py#L34)). The patch makes LlamaIndex's internal async runner work inside Jupyter notebooks by re-using the existing event loop instead of creating a new one. If no running loop is found, it falls back to `asyncio.run()`. This can interact unexpectedly with other code using LlamaIndex in the same process, particularly if that code relies on `asyncio_run` starting a clean event loop. There is currently no opt-out.

---

#### WARNING:graph_store:Retrying query in x seconds because it raised ConcurrentModificationException

While indexing data in Amazon Neptune Database, Neptune can sometimes issue a `ConcurrentModificationException`. This occurs because multiple workers are attempting to [update the same set of vertices](https://docs.aws.amazon.com/neptune/latest/userguide/transactions-exceptions.html). The GraphRAG Toolkit automatically retries transactionsb that are cancelled because of a `ConcurrentModificationException`. If the maximum number of retries is exceeded and the indexing fails, consider reducing the number of workers in the build stage using [`GraphRAGConfig.build_num_workers`](./configuration.md#graphragconfig).

---
