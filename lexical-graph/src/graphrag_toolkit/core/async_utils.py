# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Async utilities for safe coroutine execution."""

import asyncio


def run_async(coro):
    """Run a coroutine safely, whether or not an event loop is already running.
    
    Handles Jupyter notebooks and async frameworks where asyncio.run() would fail.
    """
    try:
        asyncio.get_running_loop()
        # Loop already running — execute in a new thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        # No running loop — safe to use asyncio.run()
        return asyncio.run(coro)
