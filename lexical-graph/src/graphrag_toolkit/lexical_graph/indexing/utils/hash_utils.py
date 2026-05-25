# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib

def get_hash(s):
        """
        Generates an MD5 hash for a given string.

        This method computes the MD5 hash of a provided string and returns its
        hexadecimal representation. It is used internally to generate unique hashed
        values based on string inputs.

        Args:
            s: The input string to be hashed.

        Returns:
            The hexadecimal representation of the MD5 hash of the input string.
        """
        return hashlib.md5(s.encode('utf-8'), usedforsecurity=False).digest().hex()