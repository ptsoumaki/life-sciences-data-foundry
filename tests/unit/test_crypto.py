"""Unit tests for centralized cryptographic hashing utilities (governance.crypto)."""

import hashlib

from governance.crypto import compute_sha256, is_valid_sha256


def test_compute_sha256_string_and_bytes():
    """Validates compute_sha256 on in-memory string and raw bytes."""
    data_str = "FDA-21-CFR-Part-11-Audit-Trail"
    data_bytes = data_str.encode("utf-8")

    expected_hash = hashlib.sha256(data_bytes).hexdigest()

    assert compute_sha256(data_str) == expected_hash
    assert compute_sha256(data_bytes) == expected_hash


def test_compute_sha256_file(tmp_path):
    """Validates compute_sha256 on files and Path objects."""
    test_file = tmp_path / "dataset_contract.json"
    content = b'{"dataset": "person", "contract_version": "v1.0"}'
    test_file.write_bytes(content)

    expected_hash = hashlib.sha256(content).hexdigest()

    # Pass as string path
    assert compute_sha256(str(test_file)) == expected_hash
    # Pass as Path object
    assert compute_sha256(test_file) == expected_hash


def test_is_valid_sha256():
    """Validates 64-character hexadecimal SHA-256 string verification."""
    valid_hash = hashlib.sha256(b"compliance_test").hexdigest()
    assert is_valid_sha256(valid_hash) is True
    assert is_valid_sha256(valid_hash.upper()) is True

    # Invalid cases
    assert is_valid_sha256(None) is False
    assert is_valid_sha256("") is False
    assert is_valid_sha256("not_a_hash") is False
    assert is_valid_sha256("a" * 63) is False
    assert is_valid_sha256("a" * 65) is False
    assert is_valid_sha256("g" * 64) is False  # 'g' is not hex
    assert is_valid_sha256(12345) is False  # type: ignore[arg-type]
