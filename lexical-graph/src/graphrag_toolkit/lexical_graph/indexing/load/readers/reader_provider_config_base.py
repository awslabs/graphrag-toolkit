# reader_provider_config_base.py

from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from boto3.session import Session as Boto3Session

@dataclass(init=True, kw_only=True)
class AWSReaderConfigBase(ABC):
    """
    Base configuration class for AWS-based reader providers.
    Provides Boto3 session and AWS client access.
    """
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None
    _aws_clients: Dict[str, Any] = field(default_factory=dict)
    _boto3_session: Optional[Boto3Session] = field(default=None, init=False)

    @property
    def session(self) -> Boto3Session:
        if self._boto3_session is None:
            self._boto3_session = (
                Boto3Session(profile_name=self.aws_profile, region_name=self.aws_region)
                if self.aws_profile else Boto3Session(region_name=self.aws_region)
            )
        return self._boto3_session

    def _get_or_create_client(self, service_name: str) -> Any:
        if service_name not in self._aws_clients:
            self._aws_clients[service_name] = self.session.client(service_name)
        return self._aws_clients[service_name]

    @property
    def s3(self) -> Any:
        return self._get_or_create_client("s3")

    @property
    def bedrock(self) -> Any:
        return self._get_or_create_client("bedrock")

    @property
    def sts(self) -> Any:
        return self._get_or_create_client("sts")
