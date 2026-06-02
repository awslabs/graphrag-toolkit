# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__all__ = ["LLMCache", "LLMCacheType"]

# LLMCache and LLMCacheType are imported lazily to avoid circular imports
# (config.py -> bedrock_embedding -> utils/__init__ -> llm_cache -> config.py)
def __getattr__(name):
    if name in ("LLMCache", "LLMCacheType"):
        from .llm_cache import LLMCache, LLMCacheType
        globals()["LLMCache"] = LLMCache
        globals()["LLMCacheType"] = LLMCacheType
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")