# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import boto3
import contextlib
from dataclasses import dataclass, field
from typing import Optional, Union, Dict, Set
from boto3 import Session as Boto3Session
from botocore.session import Session as BotocoreSession

from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.embeddings.bedrock import BedrockEmbedding
from llama_index.core.settings import Settings
from llama_index.core.llms import LLM
from llama_index.core.base.embeddings.base import BaseEmbedding

LLMType = Union[LLM, str]
EmbeddingType = Union[BaseEmbedding, str]

DEFAULT_EXTRACTION_MODEL = 'us.anthropic.claude-3-5-sonnet-20240620-v1:0'
DEFAULT_RESPONSE_MODEL = 'us.anthropic.claude-3-5-sonnet-20240620-v1:0'
DEFAULT_EMBEDDINGS_MODEL = 'cohere.embed-english-v3'
DEFAULT_RERANKING_MODEL = 'mixedbread-ai/mxbai-rerank-xsmall-v1'
DEFAULT_EMBEDDINGS_DIMENSIONS = 1024
DEFAULT_EXTRACTION_NUM_WORKERS = 2
DEFAULT_EXTRACTION_BATCH_SIZE = 4
DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER = 4
DEFAULT_BUILD_NUM_WORKERS = 2
DEFAULT_BUILD_BATCH_SIZE = 4
DEFAULT_BUILD_BATCH_WRITE_SIZE = 25
DEFAULT_BATCH_WRITES_ENABLED = True
DEFAULT_INCLUDE_DOMAIN_LABELS = False
DEFAULT_ENABLE_CACHE = False

def _is_json_string(s):
    try:
        json.loads(s)
        return True
    except ValueError:
        return False
    
def string_to_bool(s, default_value:bool):
    if not s:
        return default_value
    else:
        return s.lower() in ['true']

@dataclass
class _GraphRAGConfig:
    
    _aws_profile: Optional[str] = None
    _aws_region: Optional[str] = None
    _aws_clients: Dict = field(default_factory=dict)  # Use field() for mutable default

    _boto3_session: Optional[boto3.Session] = field(default=None, init=False, repr=False)
    _aws_valid_services: Optional[Set[str]] = field(default=None, init=False, repr=False)
    _session: Optional[boto3.Session] = field(default=None, init=False, repr=False)

    _extraction_llm: Optional[LLM] = None
    _response_llm: Optional[LLM] = None 
    _embed_model: Optional[BaseEmbedding] = None
    _embed_dimensions: Optional[int] = None
    _reranking_model: Optional[str] = None
    _extraction_num_workers: Optional[int] = None
    _extraction_num_threads_per_worker: Optional[int] = None
    _extraction_batch_size: Optional[int] = None
    _build_num_workers: Optional[int] = None
    _build_batch_size: Optional[int] = None
    _build_batch_write_size: Optional[int] = None
    _batch_writes_enabled: Optional[bool] = None
    _include_domain_labels: Optional[bool] = None
    _enable_cache: Optional[bool] = None

    def _get_or_create_client(self, service_name: str) -> boto3.client:
        """
        Creates or retrieves a boto3 client for a specified AWS service. This method
        maintains an internal cache of AWS clients to avoid creating multiple clients
        for the same service. If the requested client is not already cached, a new boto3
        client is created using the provided AWS region and profile, or their corresponding
        fallbacks.

        Parameters:
        service_name : str
            The name of the AWS service for which a client is required. Examples include
            's3', 'ec2', etc.

        Returns:
        boto3.client
            The boto3 client for the specified AWS service.

        Raises:
        AttributeError
            If the boto3 client cannot be created due to an error, such as invalid AWS
            credentials or an invalid service name.
        """
        if service_name in self._aws_clients:
            return self._aws_clients[service_name]

        region = self.aws_region

        profile = self.aws_profile

        try:
            if profile:
                session = boto3.Session(profile_name=profile, region_name=region)
            else:
                session = boto3.Session(region_name=region)

            client = session.client(service_name)
            self._aws_clients[service_name] = client
            return client

        except Exception as e:
            raise AttributeError(
                f"Failed to create boto3 client for '{service_name}'. "
                f"Profile: '{profile}', Region: '{region}'. "
                f"Original error: {str(e)}"
            ) from e


    @property
    def session(self) -> Boto3Session:
        """
        Initializes and manages a Boto3 session. This property lazily initializes
        a Boto3 session the first time it is accessed. It uses an explicitly
        defined AWS profile if provided or falls back to the default configuration
        or environment variables. The session is cached for future accesses unless
        explicitly reset.

        If initialization fails, it raises a runtime error containing information
        about the profile and region being used.

        Attributes:
            aws_profile (str): AWS profile name to initialize the session with. If None,
                environment variables or the default configuration will be used.
            aws_region (str): AWS region to initialize the session with.

        Returns:
            Boto3Session: An initialized Boto3 session object.

        Raises:
            RuntimeError: If the session fails to initialize due to an error.
        """
        if not hasattr(self, "_boto3_session") or self._boto3_session is None:
            try:
                # Prefer explicitly set profile
                if self.aws_profile:
                    self._boto3_session = Boto3Session(
                        profile_name=self.aws_profile,
                        region_name=self.aws_region
                    )
                else:
                    # Use environment variables or default config
                    self._boto3_session = Boto3Session(region_name=self.aws_region)

            except Exception as e:
                raise RuntimeError(
                    f"Unable to initialize boto3 session. "
                    f"Profile: {self.aws_profile}, Region: {self.aws_region}. "
                    f"Error: {e}"
                ) from e

        return self._boto3_session

    @property
    def s3(self):
        """
        Provides a read-only property to access the S3 client by retrieving or creating an
        instance of the client. The method `_get_or_create_client` is responsible for
        handling client instantiation and retrieval, ensuring the S3 client is only
        created when required.

        Returns:
            Any: The S3 client instance. The exact type is dependent on the implementation
            of `_get_or_create_client`.
        """
        return self._get_or_create_client("s3")

    @property
    def bedrock(self):
        """
        Provides a property to access the 'bedrock' client. This client is managed internally
        and created on demand, ensuring minimal resource usage unless explicitly required.

        Returns
        -------
        Any
            The 'bedrock' client instance, either previously created or newly initialized.
        """
        return self._get_or_create_client("bedrock")

    @property
    def rds(self):
        """
        Provides a property `rds` that retrieves an RDS client instance. The client
        is created if not already available, allowing users to interact with RDS
        services conveniently through the exposed interface.

        Returns
        -------
        Any
            A client instance for interacting with RDS services.
        """
        return self._get_or_create_client("rds")

    @property
    def aws_profile(self) -> Optional[str]:
        """
        Gets the AWS profile name from the environment or caches it on first call.

        This method retrieves the AWS profile currently set in the environment
        variable 'AWS_PROFILE'. If the profile name is not already cached, it
        fetches it from the environment and caches the value for future use.


        Returns:
            Optional[str]: The AWS profile name if set, otherwise None.
        """
        if self._aws_profile is None:
            self._aws_profile = os.environ.get("AWS_PROFILE")
        return self._aws_profile

    @aws_profile.setter
    def aws_profile(self, profile: str) -> None:
        """
        Sets the AWS profile to be used and clears any cached AWS clients.
        This ensures that any previously generated clients are regenerated
        with the newly set profile.

        Parameters:
            profile (str): The new AWS profile to be set.
        """
        self._aws_profile = profile
        self._aws_clients.clear()  # Clear old clients to force regeneration

    @property
    def aws_region(self) -> str:
        """Returns the AWS region, resolved from internal value or environment."""
        if self._aws_region is None:
            self._aws_region = os.environ.get("AWS_REGION", boto3.Session().region_name)
        return self._aws_region

    @aws_region.setter
    def aws_region(self, region: str) -> None:
        self._aws_region = region
        self._aws_clients.clear()  # Optional: reset clients if a region changes

    @property
    def extraction_num_workers(self) -> int:
        if self._extraction_num_workers is None:
            self.extraction_num_workers = int(os.environ.get('EXTRACTION_NUM_WORKERS', DEFAULT_EXTRACTION_NUM_WORKERS)) 

        return self._extraction_num_workers

    @extraction_num_workers.setter
    def extraction_num_workers(self, num_workers:int) -> None:
        self._extraction_num_workers = num_workers

    @property
    def extraction_num_threads_per_worker(self) -> int:
        if self._extraction_num_threads_per_worker is None:
            self.extraction_num_threads_per_worker = int(os.environ.get('EXTRACTION_NUM_THREADS_PER_WORKER', DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER)) 

        return self._extraction_num_threads_per_worker

    @extraction_num_threads_per_worker.setter
    def extraction_num_threads_per_worker(self, num_threads:int) -> None:
        self._extraction_num_threads_per_worker = num_threads

    @property
    def extraction_batch_size(self) -> int:
        if self._extraction_batch_size is None:
            self.extraction_batch_size = int(os.environ.get('EXTRACTION_BATCH_SIZE', DEFAULT_EXTRACTION_BATCH_SIZE))  

        return self._extraction_batch_size

    @extraction_batch_size.setter
    def extraction_batch_size(self, batch_size:int) -> None:
        self._extraction_batch_size = batch_size

    @property
    def build_num_workers(self) -> int:
        if self._build_num_workers is None:
            self.build_num_workers = int(os.environ.get('BUILD_NUM_WORKERS', DEFAULT_BUILD_NUM_WORKERS))

        return self._build_num_workers

    @build_num_workers.setter
    def build_num_workers(self, num_workers:int) -> None:
        self._build_num_workers = num_workers

    @property
    def build_batch_size(self) -> int:
        if self._build_batch_size is None:
            self.build_batch_size = int(os.environ.get('BUILD_BATCH_SIZE', DEFAULT_BUILD_BATCH_SIZE))  

        return self._build_batch_size

    @build_batch_size.setter
    def build_batch_size(self, batch_size:int) -> None:
        self._build_batch_size = batch_size

    @property
    def build_batch_write_size(self) -> int:
        if self._build_batch_write_size is None:
            self.build_batch_write_size = int(os.environ.get('BUILD_BATCH_WRITE_SIZE', DEFAULT_BUILD_BATCH_WRITE_SIZE))  

        return self._build_batch_write_size

    @build_batch_write_size.setter
    def build_batch_write_size(self, batch_size:int) -> None:
        self._build_batch_write_size = batch_size

    @property
    def batch_writes_enabled(self) -> bool:
        if self._batch_writes_enabled is None:
            self.batch_writes_enabled = string_to_bool(os.environ.get('BATCH_WRITES_ENABLED'), DEFAULT_BATCH_WRITES_ENABLED)

        return self._batch_writes_enabled

    @batch_writes_enabled.setter
    def batch_writes_enabled(self, batch_writes_enabled:bool) -> None:
        self._batch_writes_enabled = batch_writes_enabled

    @property
    def include_domain_labels(self) -> bool:
        if self._include_domain_labels is None:
            self.include_domain_labels = string_to_bool(os.environ.get('INCLUDE_DOMAIN_LABELS'), DEFAULT_INCLUDE_DOMAIN_LABELS)  
        return self._include_domain_labels

    @include_domain_labels.setter
    def include_domain_labels(self, include_domain_labels:bool) -> None:
        self._include_domain_labels = include_domain_labels

    @property
    def enable_cache(self) -> bool:
        if self._enable_cache is None:
            self.enable_cache = string_to_bool(os.environ.get('ENABLE_CACHE'), DEFAULT_ENABLE_CACHE)  
        return self._enable_cache

    @enable_cache.setter
    def enable_cache(self, enable_cache:bool) -> None:
        self._enable_cache = enable_cache

    def _to_llm(self, llm: LLMType):
        if isinstance(llm, LLM):
            return llm
        
        try:
            boto3_session = self.session
            botocore_session = None
            if hasattr(boto3_session, 'get_session'):
                botocore_session = boto3_session.get_session()

            profile = self.aws_profile
            region = self.aws_region

            if _is_json_string(llm):
                config = json.loads(llm)
                return BedrockConverse(
                    model=config['model'],
                    temperature=config.get('temperature', 0.0),
                    max_tokens=config.get('max_tokens', 4096),
                    botocore_session=botocore_session,
                    region_name=config.get('region_name', region),
                    profile_name=config.get('profile_name', profile)
                )

            else:
                return BedrockConverse(
                    model=llm,
                    temperature=0.0,
                    max_tokens=4096,
                    botocore_session=botocore_session,
                    region_name=region,
                    profile_name=profile
                )

        except Exception as e:
            raise ValueError(f'Failed to initialize BedrockConverse: {str(e)}') from e
   
    @property
    def extraction_llm(self) -> LLM:
        if self._extraction_llm is None:
            self.extraction_llm = os.environ.get('EXTRACTION_MODEL', DEFAULT_EXTRACTION_MODEL)
        return self._extraction_llm

    @extraction_llm.setter
    def extraction_llm(self, llm: LLMType) -> None:
        """
        Sets the extraction_llm property for the class instance. Depending on the input type, it configures an instance of
        the language model, either directly or through parsing a JSON configuration. It also integrates settings such as
        AWS session, region, and profile information.

        Parameters:
            llm (LLMType): The language model configuration which can be provided as an LLM instance, a JSON string, or
            directly as a model identifier.

        Raises:
            ValueError: Raised when the BedrockConverse initialization fails due to invalid input or processing errors.
        """

        self._extraction_llm = self._to_llm(llm)
        if hasattr(self._extraction_llm, 'callback_manager'):
            self._extraction_llm.callback_manager = Settings.callback_manager

    @property
    def response_llm(self) -> LLM:
        if self._response_llm is None:
            self.response_llm = os.environ.get('RESPONSE_MODEL', DEFAULT_RESPONSE_MODEL)    
        return self._response_llm

    @response_llm.setter
    def response_llm(self, llm: LLMType) -> None:
        """
        Setter for the response_llm attribute, allowing the setup of a language learning model
        (LLM) by interpreting input as either an instance of an LLM class, a JSON string
        representation of configuration, or a model identifier string. The method also handles
        optional configurations such as temperature, token limits, and AWS-specific details.

        Attributes:
            aws_profile: str
                The AWS profile name to be used with the LLM if specified.
            aws_region: str
                The AWS region name to be used with the LLM if specified.
            _response_llm: BedrockConverse or LLM
                The internal attribute holding the initialized LLM.

        Parameters:
            llm: LLMType
                A model object, JSON string, or model identifier to configure an LLM. May contain
                additional configuration parameters when provided as a JSON string.

        Raises:
            ValueError: If the initialization of BedrockConverse fails due to invalid input or
            other errors.
        """

        self._response_llm = self._to_llm(llm)
        if hasattr(self._response_llm, 'callback_manager'):
            self._response_llm.callback_manager = Settings.callback_manager

    @property
    def embed_model(self) -> BaseEmbedding:
        if self._embed_model is None:
            self.embed_model = os.environ.get('EMBEDDINGS_MODEL', DEFAULT_EMBEDDINGS_MODEL)

        return self._embed_model

    @embed_model.setter
    def embed_model(self, embed_model: EmbeddingType) -> None:
        if isinstance(embed_model, str):

            boto3_session = self.session
            botocore_session = None
            if hasattr(boto3_session, 'get_session'):
                botocore_session = boto3_session.get_session()

            profile = self.aws_profile
            region = self.aws_region


            if _is_json_string(embed_model):
                config = json.loads(embed_model)
                self._embed_model = BedrockEmbedding(
                    model_name=config['model_name'],
                    botocore_session=botocore_session,
                    region_name=config.get('region_name', region),
                    profile_name=config.get('profile_name', profile)
                )
            else:
                self._embed_model = BedrockEmbedding(
                    model_name=embed_model,
                    botocore_session=botocore_session,
                    region_name=region,
                    profile_name=profile
                )
        else:
            self._embed_model = embed_model

        if hasattr(self._embed_model, 'callback_manager'):
            self._embed_model.callback_manager = Settings.callback_manager
        
    @property
    def embed_dimensions(self) -> int:
       if self._embed_dimensions is None:
           self.embed_dimensions = int(os.environ.get('EMBEDDINGS_DIMENSIONS', DEFAULT_EMBEDDINGS_DIMENSIONS))
           
       return self._embed_dimensions

    @embed_dimensions.setter
    def embed_dimensions(self, embed_dimensions: int) -> None:
       self._embed_dimensions = embed_dimensions

    @property
    def reranking_model(self) -> str:
       if self._reranking_model is None:
           self._reranking_model = os.environ.get('RERANKING_MODEL', DEFAULT_RERANKING_MODEL)
           
       return self._reranking_model

    @reranking_model.setter
    def reranking_model(self, reranking_model: str) -> None:
       self._reranking_model = reranking_model
    



GraphRAGConfig = _GraphRAGConfig()