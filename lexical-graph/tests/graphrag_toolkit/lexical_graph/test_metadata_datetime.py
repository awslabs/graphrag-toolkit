# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from datetime import datetime

from graphrag_toolkit.lexical_graph.metadata import format_datetime

def test_format_datetime_strips_microseconds():
    assert format_datetime('2026-03-10T13:58:20.575327') == '2026-03-10T13:58:20'
    assert format_datetime(datetime.fromisoformat('2026-03-10T13:58:20.575327')) == '2026-03-10T13:58:20'
 