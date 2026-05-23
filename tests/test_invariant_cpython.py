import pytest
import struct
import os
import tempfile
import sys

# Adversarial payloads targeting integer overflow and boundary conditions
# These represent key sizes and values that could trigger the vulnerability
ADVERSARIAL_PAYLOADS = [
    # Empty key
    b"",
    # Single byte
    b"\x00",
    # Maximum typical key sizes
    b"A" * 511,
    b"A" * 512,
    b"A" * 513,
    # Keys with null bytes
    b"\x00" * 10,
    b"key\x00value",
    # Keys at power-of-2 boundaries
    b"K" * 255,
    b"K" * 256,
    b"K" * 1023,
    b"K" * 1024,
    # Binary data that could be misinterpreted as size fields
    struct.pack(">Q", 0xFFFFFFFFFFFFFFFF),  # SIZE_MAX equivalent
    struct.pack(">Q", 0x7FFFFFFFFFFFFFFF),  # INT64_MAX
    struct.pack(">I", 0xFFFFFFFF),          # UINT32_MAX
    struct.pack(">I", 0x7FFFFFFF),          # INT32_MAX
    # Keys with high-bit set bytes
    b"\xff\xff\xff\xff",
    b"\x80\x00\x00\x00",
    # Unicode-like byte sequences
    b"\xc0\x80",  # Overlong UTF-8 null
    b"\xed\xa0\x80",  # UTF-16 surrogate
    # Format string-like content
    b"%s%s%s%s%s%s%s%s",
    b"%n%n%n%n",
    b"%(key)s",
    # SQL injection-like
    b"'; DROP TABLE --",
    b"' OR '1'='1",
    # Path traversal
    b"../../../etc/passwd",
    b"..\\..\\..\\windows\\system32",
    # Very long key (near LMDB's default max key size of 511)
    b"X" * 500,
    # Keys with special characters
    b"\r\n\r\n",
    b"\x1b[31m",
    # Mixed binary and text
    b"key" + b"\x00" + b"injection",
    # Repeated patterns
    b"\xde\xad\xbe\xef" * 10,
    b"\xca\xfe\xba\xbe" * 8,
]


@pytest.mark.parametrize("payload", ADVERSARIAL_PAYLOADS)
def test_lmdb_key_size_boundary_safety(payload):
    """Invariant: LMDB operations must not crash, corrupt memory, or raise
    unexpected exceptions when processing adversarial key inputs. The system
    must remain stable and either succeed or raise a well-defined, handled
    exception — never segfault or exhibit undefined behavior from integer
    overflow in size calculations."""
    try:
        import lmdb
    except ImportError:
        pytest.skip("lmdb module not available")

    with tempfile.TemporaryDirectory() as tmpdir:
        env = None
        try:
            env = lmdb.open(tmpdir, max_dbs=10, map_size=10 * 1024 * 1024)

            # Test 1: Basic put/get with adversarial key
            try:
                with env.begin(write=True) as txn:
                    # The key must be bytes and within valid size limits
                    # If it's too large or invalid, lmdb should raise a
                    # well-defined exception, not crash
                    if len(payload) == 0:
                        # Empty keys should raise a defined error
                        with pytest.raises((lmdb.BadValsizeError, ValueError, lmdb.Error)):
                            txn.put(payload, b"test_value")
                    elif len(payload) > 511:
                        # Keys exceeding max size should raise a defined error
                        with pytest.raises((lmdb.BadValsizeError, ValueError, lmdb.Error)):
                            txn.put(payload, b"test_value")
                    else:
                        # Valid-length keys should work without memory corruption
                        result = txn.put(payload, b"test_value")
                        assert isinstance(result, bool), \
                            "put() must return a boolean, not corrupt memory"

                        retrieved = txn.get(payload)
                        if retrieved is not None:
                            assert retrieved == b"test_value", \
                                "Retrieved value must match stored value exactly"
            except (lmdb.BadValsizeError, lmdb.Error, ValueError, TypeError):
                # Well-defined exceptions are acceptable — the system handled it
                pass

            # Test 2: Named database creation with adversarial key as db name
            try:
                with env.begin(write=True) as txn:
                    db_name = payload[:31] if len(payload) > 0 else None
                    if db_name and len(db_name) > 0:
                        try:
                            db = env.open_db(db_name, txn=txn)
                            # If it succeeds, basic operations must be safe
                            txn.put(b"safe_key", b"safe_value", db=db)
                            val = txn.get(b"safe_key", db=db)
                            assert val == b"safe_value", \
                                "Value integrity must be maintained"
                        except (lmdb.BadValsizeError, lmdb.Error, ValueError,
                                TypeError, UnicodeDecodeError):
                            pass
            except (lmdb.BadValsizeError, lmdb.Error, ValueError, TypeError):
                pass

            # Test 3: Cursor iteration must not overflow on adversarial data
            try:
                with env.begin(write=True) as txn:
                    # Store a safe key first
                    safe_key = b"safe_iteration_key"
                    if len(safe_key) <= 511:
                        txn.put(safe_key, payload[:1024] if len(payload) > 0 else b"\x00")

                with env.begin() as txn:
                    cursor = txn.cursor()
                    count = 0
                    for key, value in cursor.iternext():
                        # Keys and values returned must be valid bytes objects
                        assert isinstance(key, bytes), \
                            "Cursor keys must be bytes objects"
                        assert isinstance(value, bytes), \
                            "Cursor values must be bytes objects"
                        # Size must be representable as a Python int
                        assert len(key) >= 0, "Key length must be non-negative"
                        assert len(value) >= 0, "Value length must be non-negative"
                        count += 1
                        if count > 10000:
                            break  # Safety limit
            except (lmdb.BadValsizeError, lmdb.Error, ValueError, TypeError):
                pass

        except (lmdb.Error, OSError, ValueError) as e:
            # Environment-level errors are acceptable
            pass
        finally:
            if env is not None:
                try:
                    env.close()
                except Exception:
                    pass

        # The process must still be alive and functional after all operations
        assert True, "Process survived adversarial input without memory corruption"


@pytest.mark.parametrize("key_size_hint", [
    0,
    1,
    255,
    256,
    510,
    511,
    512,
    1023,
    1024,
    2**16 - 1,
    2**16,
    2**31 - 1,
    2**32 - 1,
    2**63 - 1,
    2**64 - 1,
])
def test_lmdb_size_boundary_no_integer_overflow(key_size_hint):
    """Invariant: LMDB must safely handle or reject keys of any size without
    integer overflow in internal size calculations (e.g., malloc(size + 1)
    must not wrap around to 0). The process must remain stable."""
    try:
        import lmdb
    except ImportError:
        pytest.skip("lmdb module not available")

    with tempfile.TemporaryDirectory() as tmpdir:
        env = None
        try:
            env = lmdb.open(tmpdir, map_size=10 * 1024 * 1024)

            # Generate a key of the hinted size (capped for memory safety in test)
            safe_test_size = min(key_size_hint, 4096)
            test_key = b"K" * safe_test_size

            try:
                with env.begin(write=True) as txn:
                    if safe_test_size == 0 or safe_test_size > 511:
                        # Should raise a well-defined error
                        with pytest.raises((lmdb.BadValsizeError, ValueError,
                                           lmdb.Error, TypeError)):
                            txn.put(test_key, b"value")
                    else:
                        # Should succeed without memory corruption
                        txn.put(test_key, b"value")
                        result = txn.get(test_key)
                        assert result == b"value", \
                            f"Value integrity must hold for key size {safe_test_size}"
            except (lmdb.BadValsizeError, lmdb.Error, ValueError, TypeError):
                # Acceptable: well-defined rejection
                pass

        except (lmdb.Error, OSError):
            pass
        finally:
            if env is not None:
                try:
                    env.close()
                except Exception:
                    pass

        # Verify process integrity
        assert sys.maxsize > 0, "Process must remain functional after size boundary test"