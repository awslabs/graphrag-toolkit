# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import NoCredentialsError
from botocore.session import Session
from rdflib import Graph
from requests.auth import AuthBase

from .sparql_endpoint_client import RDFLibHTTPStore


class NeptuneIAMAuth(AuthBase):
    """SigV4-sign each request with the current botocore credentials.

    Calling ``get_credentials`` for every request is intentional: botocore can
    refresh temporary credentials supplied by roles, web identity, IAM Identity
    Center, and container credential providers before they expire.
    """

    def __init__(self, region_name=None, aws_session=None):
        self._session = aws_session or Session()
        configured_region = getattr(
            self._session, 'get_config_variable', lambda _: None
        )('region')
        self._region = (
            region_name
            or getattr(self._session, 'region_name', None)
            or configured_region
        )
        if not self._region:
            raise ValueError('AWS region is required for Neptune IAM authentication')

    def __call__(self, request):
        credentials = self._session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        aws_request = AWSRequest(
            method=request.method,
            url=request.url,
            data=request.body,
            headers=dict(request.headers),
        )
        SigV4Auth(
            credentials.get_frozen_credentials(), 'neptune-db', self._region
        ).add_auth(aws_request)
        request.headers.update(dict(aws_request.headers.items()))
        return request


class NeptuneIAMStore(RDFLibHTTPStore):
    """Optional IAM transport for an Amazon Neptune SPARQL endpoint."""

    def __init__(self,
                 endpoint_url,
                 region_name=None,
                 aws_session=None,
                 headers=None,
                 timeout=60.0):
        endpoint = endpoint_url.rstrip('/')
        if not endpoint.startswith('https://'):
            raise ValueError('Neptune IAM requires an HTTPS endpoint')
        if not endpoint.endswith('/sparql'):
            endpoint += '/sparql'
        super().__init__(
            endpoint,
            auth=NeptuneIAMAuth(region_name, aws_session),
            headers=headers,
            timeout=timeout,
        )


def neptune_iam_graph(endpoint_url,
                       region_name=None,
                       aws_session=None,
                       headers=None,
                       timeout=60.0):
    """Create an RDFLib Graph backed by an IAM-enabled Neptune database."""
    return Graph(
        store=NeptuneIAMStore(
            endpoint_url, region_name, aws_session, headers, timeout,
        )
    )
