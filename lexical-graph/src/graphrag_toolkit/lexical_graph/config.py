# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import time
import subprocess
import boto3
import botocore
import contextlib
from botocore import exceptions as botocore_exceptions
from botocore import configloader
from botocore.exceptions import SSOTokenLoadError

import threading
import logging
from dataclasses import dataclass, field
from typing import Optional, Union, Dict, Set, List
from boto3 import Session as Boto3Session
from botocore.config import Config

from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.embeddings.bedrock import BedrockEmbedding
from llama_index.core.settings import Settings
from llama_index.core.llms import LLM
from llama_index.core.base.embeddings.base import BaseEmbedding

LLMType = Union[LLM, str]
EmbeddingType = Union[BaseEmbedding, str]
logger = logging.getLogger(__name__)

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
DEFAULT_INCLUDE_LOCAL_ENTITIES = False
DEFAULT_ENABLE_CACHE = False
DEFAULT_METADATA_DATETIME_SUFFIXES = ['_date', '_datetime']

def _is_json_string(s):
    """
    Determines if a given string is a valid JSON string by attempting to parse it.

    This function attempts to parse the provided string as JSON. If the parsing
    is successful, it concludes that the string is a valid JSON. Otherwise,
    it considers the string invalid and returns False.

    Args:
        s (str): The string to check for JSON validity.

    Returns:
        bool: True if the input string is a valid JSON string, False otherwise.
    """
    try:
        json.loads(s)
        return True
    except ValueError:
        return False


def string_to_bool(s, default_value: bool):
    """
    Convert a string to a boolean value.

    This function converts a given string to a boolean by checking if its
    lowercase form matches 'true'. If the input string is empty or None,
    it returns the provided default boolean value.

    Args:
        s: Input string to be converted to a boolean.
        default_value: Default boolean value to return if the input string
        is None or empty.

    Returns:
        A boolean value - True if the string equals 'true' (case-insensitive),
        False otherwise, or the default_value if the input string is empty
        or None.
    """
    return s.lower() in ['true'] if s else default_value

class ResilientClient:
    """
    A wrapper for boto3 clients that automatically refreshes credentials when they expire.
    
    This class proxies method calls to an underlying boto3 client while adding resilience
    against credential expiration. When an AWS operation fails due to expired credentials,
    the client is automatically refreshed and the operation is retried.
    
    Attributes:
        config: The GraphRAGConfig instance that provides session management
        service_name: The AWS service name for this client (e.g., 's3', 'bedrock')
        _client: The underlying boto3 client instance
        _lock: Thread lock to ensure thread-safe client refreshing
    """
    def __init__(self, config, service_name):
        """
        Initialize a new resilient client for the specified AWS service.
        
        Args:
            config: The GraphRAGConfig instance that provides session management
            service_name: The AWS service name to create a client for
        """
        self.config = config
        self.service_name = service_name
        self._client = self._create_client()
        self._lock = threading.Lock()


    def _create_client(self):
        """
        Create a new boto3 client using the config's session.

        Returns:
            boto3.client: A new boto3 client for the specified service

        Raises:
            RuntimeError: If the AWS SSO token is missing or expired
        """
        try:
            return self.config.session.client(self.service_name)
        except SSOTokenLoadError as e:
            raise RuntimeError(
                f"[ResilientClient] SSO token is missing or expired for profile '{self.config.aws_profile}'.\n"
                f"Please run: aws sso login --profile {self.config.aws_profile}\n\n"
                f"Original error: {str(e)}"
            ) from e

    @staticmethod
    def _is_expired(error):
        """
        Check if an AWS error is due to expired credentials.
        
        Args:
            error: The botocore ClientError to check
            
        Returns:
            bool: True if the error indicates expired credentials, False otherwise
        """
        error_code = getattr(error, 'response', {}).get('Error', {}).get('Code', '')
        return error_code in ['ExpiredToken', 'RequestExpired', 'InvalidClientTokenId']
    
    @contextlib.contextmanager
    def _refreshing_lock(self):
        """
        Context manager for thread-safe operations.
        
        Acquires the lock before yielding control and ensures the lock
        is released even if an exception occurs.
        
        Yields:
            None
        """
        with self._lock:
            yield
        
    def _refresh_client(self):
        """
        Refresh the underlying boto3 client in a thread-safe manner.
        
        This method uses a context manager to ensure thread safety when
        multiple threads encounter expired credentials simultaneously.
        """
        with self._refreshing_lock():
            self._client = self._create_client()

    @contextlib.contextmanager
    def _handle_credential_expiration(self, method_name):
        """
        Context manager for handling credential expiration during AWS API calls.

        If credentials are expired or the SSO token is missing, it refreshes the
        client or raises a clear message.
        """
        try:
            yield
        except SSOTokenLoadError as e:
            raise RuntimeError(
                f"[ResilientClient] SSO token is missing or expired for profile '{self.config.aws_profile}'.\n"
                f"Please run: aws sso login --profile {self.config.aws_profile}\n\n"
                f"Original error: {e}"
            ) from e
        except botocore_exceptions.ClientError as e:
            if self._is_expired(e):
                logger.warning(f"[ResilientClient] Refreshing expired client for {self.service_name}")
                self._refresh_client()
            else:
                raise

    def __getattr__(self, name):
        """
        Proxy attribute access to the underlying boto3 client.
        
        This method intercepts method calls to the boto3 client and wraps them
        with automatic credential refresh logic. If a method call fails due to
        expired credentials, the client is refreshed and the call is retried.
        
        Args:
            name: The name of the attribute being accessed
            
        Returns:
            The attribute from the underlying client, or a wrapped method that
            handles credential expiration
        """
        attr = getattr(self._client, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                with self._handle_credential_expiration(name):
                    try:
                        return attr(*args, **kwargs)
                    except botocore_exceptions.ClientError as e:
                        if self._is_expired(e):
                            # The context manager has refreshed the client, retry with the new client
                            return getattr(self._client, name)(*args, **kwargs)
                        raise
            return wrapper
        return attr
@dataclass
class _GraphRAGConfig:
    """
    Configuration class for managing parameters and clients in a Graph-based RAG (Retrieve and Generate)
    system. This class encapsulates the configuration necessary for interacting with AWS services, LLM-based
    extractions, and embeddable models while providing utility properties and methods to simplify access
    and management.

    Attributes:
        _aws_profile (Optional[str]): The AWS profile name to be utilized for authentication.
        _aws_region (Optional[str]): The AWS region to be used for service access.
        _aws_clients (Dict): Caches AWS clients for reusability and efficiency.
        _boto3_session (Optional[boto3.Session]): The internal boto3 session, initialized on demand.
        _aws_valid_services (Optional[Set[str]]): A set of valid AWS service names for validation purposes.
        _session (Optional[boto3.Session]): Boto3 session attribute initialized or reused.
        _extraction_llm (Optional[LLM]): The LLM configured for extraction tasks.
        _response_llm (Optional[LLM]): The LLM configured for response generation tasks.
        _embed_model (Optional[BaseEmbedding]): An embedding model for vector generation.
        _embed_dimensions (Optional[int]): The dimensions of the embedding vectors.
        _reranking_model (Optional[str]): A string specifying the reranking model to use.
        _extraction_num_workers (Optional[int]): Number of parallel workers for extraction tasks.
        _extraction_num_threads_per_worker (Optional[int]): Number of threads per worker in extraction operations.
        _extraction_batch_size (Optional[int]): The batch size for processing during extraction.
        _build_num_workers (Optional[int]): The number of workers used when building structures or data.
        _build_batch_size (Optional[int]): The size of data batches processed during build operations.
        _build_batch_write_size (Optional[int]): Limit for the size of batch writes in the build process.
        _batch_writes_enabled (Optional[bool]): Flag indicating whether batch writes are enabled.
        _include_domain_labels (Optional[bool]): Whether domain-specific labels are included in processes.
        _include_local_entities (Optional[bool]): Whether local entities are included in the graph.
        _enable_cache (Optional[bool]): Boolean flag to enable or disable caching mechanisms.
        _metadata_datetime_suffixes (Optional[List[str]]): List of datetime suffixes included in metadata handling.
    """
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
    _include_local_entities: Optional[bool] = None
    _enable_cache: Optional[bool] = None
    _metadata_datetime_suffixes: Optional[List[str]] = None

    @contextlib.contextmanager
    def _validate_sso_token(self, profile):
        """
        Context manager for SSO token validation.
        
        This method checks if the AWS profile uses SSO authentication and validates
        the SSO token, refreshing it if necessary. Any exceptions during this process
        are suppressed to ensure the main client creation flow continues.
        
        Args:
            profile: The AWS profile name to check for SSO configuration
            
        Yields:
            None
        """
        try:
            config = configloader.load_config('~/.aws/config')
            profile_config = config.get('profiles', {}).get(profile, {})
            
            if 'sso_start_url' in profile_config:
                cache_path = os.path.expanduser('~/.aws/sso/cache')
                token_valid = False
                
                if os.path.exists(cache_path):
                    token_valid = any(
                        os.path.isfile(os.path.join(cache_path, f)) and 
                        os.path.getmtime(os.path.join(cache_path, f)) > time.time() - 3600 
                        for f in os.listdir(cache_path)
                    )
                    
                if not token_valid:
                    subprocess.run(['aws', 'sso', 'login', '--profile', profile], check=True)
            yield
        except Exception:
            # Continue even if SSO validation fails
            yield
    
    def _get_or_create_client(self, service_name: str):
        """
        Creates or retrieves a resilient boto3 client for a specified AWS service.
        
        This method maintains an internal cache of AWS clients to avoid creating multiple
        clients for the same service. If the requested client is not already cached, a new
        ResilientClient is created which wraps a boto3 client with automatic token refresh
        capabilities.
        
        The method also handles SSO authentication by checking if the configured profile
        uses SSO and validating/refreshing the SSO token if necessary before creating
        the client.
        
        Args:
            service_name: The name of the AWS service for which a client is required
                (e.g., 's3', 'bedrock', 'rds')
            
        Returns:
            ResilientClient: A resilient client for the specified AWS service that
                automatically handles credential refresh
            
        Raises:
            AttributeError: If client creation fails due to invalid credentials,
                insufficient permissions, or other AWS-related errors
        """
        if service_name in self._aws_clients:
            return self._aws_clients[service_name]

        profile = self.aws_profile

        # Check if profile uses SSO and validate token if needed
        if profile:
            with self._validate_sso_token(profile):
                pass
        # Create the client with error handling
        try:
            resilient_client = ResilientClient(self, service_name)
            self._aws_clients[service_name] = resilient_client
            return resilient_client
        except Exception as e:
            raise AttributeError(
                f"Failed to create boto3 client for '{service_name}'. "
                f"Profile: '{profile}', Region: '{self.aws_region}'. "
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
        """
        Sets the AWS region to be used for AWS service interactions. Assigning a new region
        will automatically reset cached AWS clients, ensuring that subsequent AWS service
        requests are directed to the specified region.

        Args:
            region (str): The AWS region to be set, e.g., "us-west-1", "eu-central-1".
        """
        self._aws_region = region
        self._aws_clients.clear()  # Optional: reset clients if a region changes

    @property
    def extraction_num_workers(self) -> int:
        """
        Retrieves the number of workers assigned for the extraction process.

        If no value has been explicitly set, it assigns and returns the default
        number of workers, which is obtained from the environment variable
        `EXTRACTION_NUM_WORKERS`. If the environment variable is not set, the
        default value of `DEFAULT_EXTRACTION_NUM_WORKERS` is used.

        Returns:
            int: The number of extraction workers.
        """
        if self._extraction_num_workers is None:
            self.extraction_num_workers = int(os.environ.get('EXTRACTION_NUM_WORKERS', DEFAULT_EXTRACTION_NUM_WORKERS))

        return self._extraction_num_workers

    @extraction_num_workers.setter
    def extraction_num_workers(self, num_workers: int) -> None:
        """
        Sets the number of workers used for the extraction process.

        The `extraction_num_workers` setter method allows configuring the number
        of concurrent workers available for the extraction process. It ensures
        that the provided value is stored in the internal `_extraction_num_workers`
        attribute.

        Args:
            num_workers (int): The number of workers to be utilized for extraction.
        """
        self._extraction_num_workers = num_workers

    @property
    def extraction_num_threads_per_worker(self) -> int:
        """
        Gets the number of threads allocated per worker for the extraction process.

        This property retrieves the number of threads allocated per worker for data
        extraction. If the number of threads has not been manually set, it defaults
        to the value specified in the environment variable
        'EXTRACTION_NUM_THREADS_PER_WORKER'. If the environment variable is not set,
        it will use the application's default value defined by
        `DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER`.

        Returns:
            int: The number of threads allocated per worker for extraction.
        """
        if self._extraction_num_threads_per_worker is None:
            self.extraction_num_threads_per_worker = int(
                os.environ.get('EXTRACTION_NUM_THREADS_PER_WORKER', DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER))

        return self._extraction_num_threads_per_worker

    @extraction_num_threads_per_worker.setter
    def extraction_num_threads_per_worker(self, num_threads: int) -> None:
        """
        Sets the number of extraction threads per worker.

        This setter method updates the number of threads to be used for
        extraction processes per worker. It ensures that the configured
        number of threads can be dynamically changed for optimization
        or resource management purposes.

        Args:
            num_threads (int): The number of threads to be allocated for
                each worker during extraction operations.
        """
        self._extraction_num_threads_per_worker = num_threads

    @property
    def extraction_batch_size(self) -> int:
        """
        Gets or sets the batch size for data extraction operations. The batch size is determined
        by an environment variable 'EXTRACTION_BATCH_SIZE'. If this variable is not set, a default
        value 'DEFAULT_EXTRACTION_BATCH_SIZE' is applied. This property ensures that the batch size
        is retrieved or initialized appropriately whenever accessed.

        Returns:
            int: The configured batch size for extraction operations. If not already configured,
            initializes it using an environment variable or a default value.
        """
        if self._extraction_batch_size is None:
            self.extraction_batch_size = int(os.environ.get('EXTRACTION_BATCH_SIZE', DEFAULT_EXTRACTION_BATCH_SIZE))

        return self._extraction_batch_size

    @extraction_batch_size.setter
    def extraction_batch_size(self, batch_size: int) -> None:
        """
        Sets the extraction batch size for the process.

        The extraction batch size determines the number of items to be
        processed in each batch. This allows for customization of workload
        distribution based on available resources and specific performance
        considerations for the task.

        Args:
            batch_size (int): The size of the extraction batch. Must be an
                integer representing the number of items in a batch.
        """
        self._extraction_batch_size = batch_size

    @property
    def build_num_workers(self) -> int:
        """
        Gets the number of workers to be used for the build process. This property fetches
        the value from the environment variable `BUILD_NUM_WORKERS`, if available, or
        defaults to a predefined constant `DEFAULT_BUILD_NUM_WORKERS`. The value is cached
        for subsequent accesses.

        Returns:
            int: The number of workers to be used for the build process.
        """
        if self._build_num_workers is None:
            self.build_num_workers = int(os.environ.get('BUILD_NUM_WORKERS', DEFAULT_BUILD_NUM_WORKERS))

        return self._build_num_workers

    @build_num_workers.setter
    def build_num_workers(self, num_workers: int) -> None:
        """
        Sets the number of workers to be used for building processes.

        This setter method assigns the provided value to the internal
        attribute _build_num_workers, which represents the number of
        workers utilized in building or processing tasks.

        Args:
            num_workers (int): The number of workers to be used. This value
                determines the concurrency level during execution.
        """
        self._build_num_workers = num_workers

    @property
    def build_batch_size(self) -> int:
        """
        Gets the build batch size for the process. The build batch size can be dynamically
        retrieved from an environment variable or falls back to a default value if not set.

        Returns:
            int: The batch size used during the build process.
        """
        if self._build_batch_size is None:
            self.build_batch_size = int(os.environ.get('BUILD_BATCH_SIZE', DEFAULT_BUILD_BATCH_SIZE))

        return self._build_batch_size

    @build_batch_size.setter
    def build_batch_size(self, batch_size: int) -> None:
        """
        Sets the batch size for the build process.

        This property setter method allows updating the current batch size, which
        is used during the build process. The value must be an integer.

        Args:
            batch_size: The batch size to be set for the build process. Must be a
                positive integer indicating the number of items per batch.
        """
        self._build_batch_size = batch_size

    @property
    def build_batch_write_size(self) -> int:
        """
        Gets the batch write size for the build process.

        This property retrieves the value of `_build_batch_write_size`. If its value is
        `None`, it sets `_build_batch_write_size` to the integer value specified in the
        environment variable `BUILD_BATCH_WRITE_SIZE`. If this environment variable is not
        set, it defaults to the value of `DEFAULT_BUILD_BATCH_WRITE_SIZE`.

        Returns:
            int: The configured batch write size for the build process.
        """
        if self._build_batch_write_size is None:
            self.build_batch_write_size = int(os.environ.get('BUILD_BATCH_WRITE_SIZE', DEFAULT_BUILD_BATCH_WRITE_SIZE))

        return self._build_batch_write_size

    @build_batch_write_size.setter
    def build_batch_write_size(self, batch_size: int) -> None:
        """
        Sets the size of the batch for writing during the build process. This property
        controls how many entries are processed in a single batch when writing data.

        Args:
            batch_size: Number of entries in a single batch for the write process.
        """
        self._build_batch_write_size = batch_size

    @property
    def batch_writes_enabled(self) -> bool:
        """
        Determines whether batch writes are enabled based on an environment variable.

        The `batch_writes_enabled` property retrieves the value indicating whether batch
        writes are enabled. By default, it fetches and converts the value of the
        environment variable `BATCH_WRITES_ENABLED`. If this environment variable is not
        set, it will use the `DEFAULT_BATCH_WRITES_ENABLED` constant as the default value.
        The property automatically converts the environment variable value to a boolean.

        Attributes:
            _batch_writes_enabled: Cached value for the batch writes enabled status,
                computed once based on the environment variable.

        Returns:
            bool: A boolean value indicating whether batch writes are enabled.
        """
        if self._batch_writes_enabled is None:
            self.batch_writes_enabled = string_to_bool(os.environ.get('BATCH_WRITES_ENABLED'),
                                                       DEFAULT_BATCH_WRITES_ENABLED)

        return self._batch_writes_enabled

    @batch_writes_enabled.setter
    def batch_writes_enabled(self, batch_writes_enabled: bool) -> None:
        self._batch_writes_enabled = batch_writes_enabled

    @property
    def include_domain_labels(self) -> bool:
        """
        Property to retrieve or compute the value indicating whether domain labels
        should be included. The value is initially derived from an environment variable
        and a default setting. The environment variable `INCLUDE_DOMAIN_LABELS` is
        checked, and its string value is converted to a boolean. If not provided, the
        default value `DEFAULT_INCLUDE_DOMAIN_LABELS` is used. Once computed, the value
        is cached for subsequent accesses.

        Returns:
            bool: The value indicating whether domain labels should be included.
        """
        if self._include_domain_labels is None:
            self.include_domain_labels = string_to_bool(os.environ.get('INCLUDE_DOMAIN_LABELS'),
                                                        DEFAULT_INCLUDE_DOMAIN_LABELS)
        return self._include_domain_labels

    @include_domain_labels.setter
    def include_domain_labels(self, include_domain_labels: bool) -> None:
        """
        Setter for the `include_domain_labels` attribute of the class.

        This setter method assigns a value to the underlying private
        attribute `_include_domain_labels`, which stores a boolean value
        indicating whether domain labels should be included.

        Args:
            include_domain_labels (bool): A boolean specifying whether domain
                labels should be included.
        """
        self._include_domain_labels = include_domain_labels

    @property
    def include_local_entities(self) -> bool:   
        if self._include_local_entities is None:
            self.include_local_entities = string_to_bool(os.environ.get('INCLUDE_LOCAL_ENTITIES'), DEFAULT_INCLUDE_LOCAL_ENTITIES)
        return self._include_local_entities

    @include_local_entities.setter
    def include_local_entities(self, include_local_entities: bool) -> None:
        self._include_local_entities = include_local_entities

    @property
    def enable_cache(self) -> bool:
        """
        Indicates whether the caching mechanism is enabled for the application. This
        property evaluates an environment variable to determine the cache status. If
        the environment variable is not set, a default value is used.

        Attributes:
            _enable_cache (bool, optional): Internal cache value that stores the
                evaluated result of the `ENABLE_CACHE` environment variable.

        Returns:
            bool: True if caching is enabled, False otherwise.
        """
        if self._enable_cache is None:
            self.enable_cache = string_to_bool(os.environ.get('ENABLE_CACHE'), DEFAULT_ENABLE_CACHE)
        return self._enable_cache

    @enable_cache.setter
    def enable_cache(self, enable_cache: bool) -> None:
        """
        Sets the value of the enable_cache attribute.

        This property setter allows modifying the internal `_enable_cache`
        attribute, which likely controls whether caching is enabled or
        disabled within the class where this property is defined.

        Args:
            enable_cache: New value to assign to the `_enable_cache` attribute.
        """
        self._enable_cache = enable_cache

    @property
    def metadata_datetime_suffixes(self) -> List[str]:
        """
        Gets the list of datetime suffixes for metadata.

        This property retrieves the list of datetime suffixes used in metadata.
        If the value has not been set explicitly, it defaults to a predefined
        set of datetime suffixes.

        Returns:
            List[str]: A list of datetime suffix strings.

        Raises:
            None.
        """
        if self._metadata_datetime_suffixes is None:
            self.metadata_datetime_suffixes = DEFAULT_METADATA_DATETIME_SUFFIXES
        return self._metadata_datetime_suffixes

    @metadata_datetime_suffixes.setter
    def metadata_datetime_suffixes(self, metadata_datetime_suffixes: List[str]) -> None:
        """
        Sets the metadata datetime suffixes for the instance.

        This property setter updates the list of metadata datetime suffixes used
        for customization or configuration within the instance. These suffixes
        are adjustable to cater to specific metadata requirements.

        Args:
            metadata_datetime_suffixes (List[str]): A list containing suffixes for
                metadata datetime configurations.

        """
        self._metadata_datetime_suffixes = metadata_datetime_suffixes

    def _to_llm(self, llm: LLMType):
        """
        Converts the given LLMType into an instance of LLM or BedrockConverse.

        The method accepts an LLM or a string representation of configuration,
        and converts it to an appropriate instance of BedrockConverse based
        on the provided details. If `llm` is already an instance of LLM, it
        is returned directly. When `llm` is a valid JSON string, a
        BedrockConverse instance is initialized with the extracted
        configuration. Otherwise, a default BedrockConverse instance
        is created using specified attributes such as AWS profile and
        region.

        Args:
            llm: An instance of LLMType which could be an LLM instance,
                a JSON string containing configuration details, or a simple
                string representing the model.

        Returns:
            LLM: The processed LLM instance or an instance of BedrockConverse
            initialized based on the provided parameters.

        Raises:
            ValueError: If BedrockConverse initialization fails due to
                invalid input or unexpected errors during processing.
        """
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
                    profile_name=config.get('profile_name', profile),
                    max_retries=50
                )

            else:
                return BedrockConverse(
                    model=llm,
                    temperature=0.0,
                    max_tokens=4096,
                    botocore_session=botocore_session,
                    region_name=region,
                    profile_name=profile,
                    max_retries=50
                )

        except Exception as e:
            raise ValueError(f'Failed to initialize BedrockConverse: {str(e)}') from e

    @property
    def extraction_llm(self) -> LLM:
        """
        Property to retrieve or initialize the LLM (Language Learning Model) for extraction.

        Provides access to the `LLM` instance used for data extraction tasks. If the
        extraction model has not previously been set, it initializes the model based on
        the environment variable `EXTRACTION_MODEL`. If the environment variable is not
        defined, it defaults to the `DEFAULT_EXTRACTION_MODEL`.

        Attributes:
            extraction_llm (LLM): The LLM instance utilized for extraction purposes.

        Returns:
            LLM: The language learning model used for extraction workflows.
        """
        if self._extraction_llm is None:
            self.extraction_llm = os.environ.get('EXTRACTION_MODEL', DEFAULT_EXTRACTION_MODEL)
        return self._extraction_llm

    @extraction_llm.setter
    def extraction_llm(self, llm: LLMType) -> None:
        """Sets the extraction_llm property with a given LLM instance.

        This setter method assigns the provided LLM instance to the
        internal `_extraction_llm` attribute after processing it via
        a helper method `_to_llm`. Additionally, if the provided LLM
        supports a `callback_manager` attribute, it is set to use
        the global `Settings.callback_manager`.

        Args:
            llm: An instance of LLMType that represents the language
                model to be set.
        """

        self._extraction_llm = self._to_llm(llm)
        if hasattr(self._extraction_llm, 'callback_manager'):
            self._extraction_llm.callback_manager = Settings.callback_manager

    @property
    def response_llm(self) -> LLM:
        """
        Gets the response language model (LLM) instance. If the response LLM is not already set, it initializes it
        using the value of the 'RESPONSE_MODEL' environment variable; if not defined, defaults to
        `DEFAULT_RESPONSE_MODEL`. This ensures lazy initialization of the response LLM.

        Attributes:
            response_llm (LLM): Instance of the response language model. If it is not
                defined, it retrieves the value from the environment or the default value.

        Returns:
            LLM: The response language model instance.
        """
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
        """
        Property that retrieves the embedding model used for processing data.

        This property either retrieves the existing embedding model or initializes
        it based on the environment variable `EMBEDDINGS_MODEL`. If the environment
        variable is not set, a default embedding model defined by
        `DEFAULT_EMBEDDINGS_MODEL` is used. This allows for flexible and configurable
        model initialization.

        Returns:
            BaseEmbedding: The embedding model instance.
        """
        if self._embed_model is None:
            self.embed_model = os.environ.get('EMBEDDINGS_MODEL', DEFAULT_EMBEDDINGS_MODEL)

        return self._embed_model

    @embed_model.setter
    def embed_model(self, embed_model: EmbeddingType) -> None:
        """
        Sets the embed_model attribute of the instance. The embed_model can either
        be a string (indicating the name or configuration of the embedding model)
        or an instance of an embedding type. Depending on the input, it configures
        or assigns the specified embedding model along with AWS-related settings
        and session management.

        Args:
            embed_model (EmbeddingType): Specifies the embedding model to be used.
                It can be provided as a string (indicating either the model name or
                a JSON string containing detailed configuration) or as an
                EmbeddingType instance.

        Raises:
            ValueError: If a JSON string provided via `embed_model` does not conform
                to the expected structure or lacks required fields during parsing.
        """
        if isinstance(embed_model, str):

            boto3_session = self.session
            botocore_session = None
            if hasattr(boto3_session, 'get_session'):
                botocore_session = boto3_session.get_session()

            profile = self.aws_profile
            region = self.aws_region

            botocore_config = Config(
                retries={"total_max_attempts": 10, "mode": "adaptive"},
                connect_timeout=60.0,
                read_timeout=60.0,
            )

            if _is_json_string(embed_model):
                config = json.loads(embed_model)
                self._embed_model = BedrockEmbedding(
                    model_name=config['model_name'],
                    botocore_session=botocore_session,
                    region_name=config.get('region_name', region),
                    profile_name=config.get('profile_name', profile),
                    botocore_config=botocore_config
                )
            else:
                self._embed_model = BedrockEmbedding(
                    model_name=embed_model,
                    botocore_session=botocore_session,
                    region_name=region,
                    profile_name=profile,
                    botocore_config=botocore_config
                )
        else:
            self._embed_model = embed_model

        if hasattr(self._embed_model, 'callback_manager'):
            self._embed_model.callback_manager = Settings.callback_manager

    @property
    def embed_dimensions(self) -> int:
        """
        Gets the dimensions of embeddings.

        This property retrieves the dimensionality of embeddings used in a system.
        If the value is not currently set, it will lazily initialize by fetching the
        value from the environment variable 'EMBEDDINGS_DIMENSIONS', falling back to
        a predefined default value if the environment variable is not set.

        Returns:
            int: The dimensions of embeddings.
        """
        if self._embed_dimensions is None:
            self.embed_dimensions = int(os.environ.get('EMBEDDINGS_DIMENSIONS', DEFAULT_EMBEDDINGS_DIMENSIONS))

        return self._embed_dimensions

    @embed_dimensions.setter
    def embed_dimensions(self, embed_dimensions: int) -> None:
        """
        Sets the embed_dimensions attribute of the object.

        This setter method assigns the value provided to the private
        attribute _embed_dimensions. It is expected that the value is
        an integer specifying dimensions for embedding.

        Args:
            embed_dimensions (int): The embedding dimensions to be
                assigned to the instance.
        """
        self._embed_dimensions = embed_dimensions

    @property
    def reranking_model(self) -> str:
        """
        Gets the reranking model used for the system. The model is retrieved from an
        environment variable if available, otherwise a default model is used. This allows for
        flexibility and configurability in selecting the reranking model without hardcoding
        the value.

        Returns:
            str: The name or identifier of the reranking model being used.
        """
        if self._reranking_model is None:
            self._reranking_model = os.environ.get('RERANKING_MODEL', DEFAULT_RERANKING_MODEL)

        return self._reranking_model

    @reranking_model.setter
    def reranking_model(self, reranking_model: str) -> None:
        """
        Sets the reranking model used for the process. This allows the user to customize the underlying
        model used for reranking operations within the application. The process ensures the provided
        value is stored internally for consistent accessibility.

        Args:
            reranking_model (str): The name or identifier for the reranking model to be used.
        """
        self._reranking_model = reranking_model


GraphRAGConfig = _GraphRAGConfig()