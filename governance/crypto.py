"""Cryptographic hashing and digest verification utilities for FDA 21 CFR Part 11 compliance."""

import hashlib
import os
import re
from pathlib import Path


def compute_sha256(target: str | bytes | Path) -> str:
    """Computes a cryptographic SHA-256 checksum for a file path, string, or byte sequence.

    Args:
        target: File path, string content, or raw bytes to hash.

    Returns:
        Hexadecimal 64-character SHA-256 digest string.
    """
    if isinstance(target, (str, Path)) and os.path.isfile(target):
        sha256_hash = hashlib.sha256()
        with open(target, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(block)
        return sha256_hash.hexdigest()

    content = target if isinstance(target, bytes) else str(target).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def is_valid_sha256(hash_str: str | None) -> bool:
    """Validates whether a string conforms to a 64-character hexadecimal SHA-256 digest.

    Args:
        hash_str: The candidate hash string to validate.

    Returns:
        True if valid SHA-256 hex string, False otherwise.
    """
    if not hash_str or not isinstance(hash_str, str):
        return False
    return bool(re.match(r"^[a-fA-F0-9]{64}$", hash_str.strip()))
