from abc import ABC
from dataclasses import dataclass
from typing import Optional

@dataclass
class ReaderProviderConfig(ABC):
    """Base configuration class for all reader providers."""
    pass

@dataclass
class AWSReaderConfigBase(ReaderProviderConfig):
    """Base configuration class for AWS-based reader providers."""
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None
    
    def get_boto3_session(self):
        """Get boto3 session with configured profile/region."""
        from boto3.session import Session as Boto3Session
        return (
            Boto3Session(profile_name=self.aws_profile, region_name=self.aws_region)
            if self.aws_profile else Boto3Session(region_name=self.aws_region)
        )