"""The gzip stream joiner must produce standard gzip that any client decodes to head + tail."""

import gzip
import os
import random
import subprocess
import zlib

import pytest

from live_core.gzipjoin import CompressedTail, _shift_operator, crc32_combine, gzip_join


def test_crc32_combine_matches_zlib():
    rng = random.Random(7)
    for _ in range(200):
        a = os.urandom(rng.randint(0, 2000))
        b = os.urandom(rng.randint(0, 70000))
        assert crc32_combine(zlib.crc32(a), zlib.crc32(b), len(b)) == zlib.crc32(a + b)
        if b:
            assert crc32_combine(zlib.crc32(a), zlib.crc32(b), len(b), _shift_operator(len(b))) == zlib.crc32(a + b)


@pytest.mark.parametrize("pieces", [0, 1, 5, 400])
def test_joined_stream_is_ordinary_gzip(pieces, tmp_path):
    rng = random.Random(pieces)
    head = b'{"service":"PSYGRID","session":{"current_time_ist":"x"},"stocks":{'
    parts = [bytes(rng.choice(b'{}":,0123456789.abc') for _ in range(rng.randint(0, 9000))) for _ in range(pieces)]
    tail = CompressedTail(parts)
    joined = gzip_join(head, tail)
    assert gzip.decompress(joined) == head + b"".join(parts)
    assert zlib.decompress(joined, 16 + zlib.MAX_WBITS) == head + b"".join(parts)
    assert tail.data == b"".join(parts)
    path = tmp_path / "joined.gz"
    path.write_bytes(joined)
    assert subprocess.run(["gzip", "-t", str(path)], check=False).returncode == 0
    # The same tail serves many heads (one per response).
    assert gzip.decompress(gzip_join(b'{"other":1,"stocks":{', tail)) == b'{"other":1,"stocks":{' + b"".join(parts)


def test_tail_keeps_references_not_copies():
    fragment = b"x" * 100_000
    tail = CompressedTail([fragment, fragment])
    assert tail.parts[0] is fragment and tail._data is None  # no joined copy until asked for
