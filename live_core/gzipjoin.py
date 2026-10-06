"""Gzip a small, per-request head in front of a large body compressed once.

A live response is ``head + tail``: the head (status, session time, coverage) changes on every
request, the tail (989 stocks of candles, ~40 MB at the close) only when a minute completes. The
tail is deflated once; each request deflates just its head with a full flush (byte-aligned, no
back-references across the join) and splices the two raw deflate streams into one gzip member.
The gzip CRC-32 of head+tail is combined arithmetically from the two parts' CRCs, so the tail is
never re-read. The result is an ordinary single-member gzip stream any client can decode.
"""

from __future__ import annotations

import struct
import threading
import zlib

LEVEL = 3
_GZIP_HEADER = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff"  # deflate, no flags, mtime 0, unknown OS


def _gf2_times(matrix: list[int], vector: int) -> int:
    result = 0
    index = 0
    while vector:
        if vector & 1:
            result ^= matrix[index]
        vector >>= 1
        index += 1
    return result


def _gf2_square(matrix: list[int]) -> list[int]:
    return [_gf2_times(matrix, matrix[n]) for n in range(32)]


def _shift_operator(length: int) -> list[int]:
    """GF(2) matrix that advances a CRC-32 over ``length`` zero bytes (zlib's crc32_combine core)."""
    result = [1 << n for n in range(32)]  # identity
    odd = [0xEDB88320] + [1 << n for n in range(31)]
    even = _gf2_square(odd)
    odd = _gf2_square(even)
    while length:
        even = _gf2_square(odd)
        if length & 1:
            result = [_gf2_times(even, column) for column in result]
        length >>= 1
        if not length:
            break
        odd = _gf2_square(even)
        if length & 1:
            result = [_gf2_times(odd, column) for column in result]
        length >>= 1
    return result


def crc32_combine(crc1: int, crc2: int, length2: int, operator: list[int] | None = None) -> int:
    """CRC-32 of A+B from crc(A), crc(B) and len(B). ``operator`` may be a cached _shift_operator(len(B))."""
    if length2 <= 0:
        return crc1
    operator = operator if operator is not None else _shift_operator(length2)
    return _gf2_times(operator, crc1) ^ crc2


class CompressedTail:
    """A large, immutable response tail: its CRC, length and raw deflate stream are computed once.

    The tail is kept as the list of existing fragment ``bytes`` it is made of (no joined copy); a
    contiguous copy is made only if an uncompressed response is actually requested.
    """

    __slots__ = ("_data", "_deflated", "_lock", "_operator", "crc", "length", "parts")

    def __init__(self, parts):
        self.parts = [parts] if isinstance(parts, (bytes, bytearray)) else list(parts)
        crc = 0
        length = 0
        for part in self.parts:
            crc = zlib.crc32(part, crc)
            length += len(part)
        self.crc = crc
        self.length = length
        self._operator: list[int] | None = None
        self._deflated: bytes | None = None
        self._data: bytes | None = None
        self._lock = threading.Lock()

    @property
    def data(self) -> bytes:
        with self._lock:
            if self._data is None:
                self._data = b"".join(self.parts)
            return self._data

    def operator(self) -> list[int]:
        with self._lock:
            if self._operator is None:
                self._operator = _shift_operator(self.length)
            return self._operator

    def deflated(self) -> bytes:
        with self._lock:
            if self._deflated is None:
                compressor = zlib.compressobj(LEVEL, zlib.DEFLATED, -15)
                chunks = [compressor.compress(part) for part in self.parts]
                chunks.append(compressor.flush(zlib.Z_FINISH))
                self._deflated = b"".join(chunks)
            return self._deflated


def gzip_join(head: bytes, tail: CompressedTail) -> bytes:
    compressor = zlib.compressobj(LEVEL, zlib.DEFLATED, -15)
    head_stream = compressor.compress(head) + compressor.flush(zlib.Z_FULL_FLUSH)
    crc = crc32_combine(zlib.crc32(head), tail.crc, tail.length, tail.operator())
    trailer = struct.pack("<II", crc, (len(head) + tail.length) & 0xFFFFFFFF)
    return b"".join((_GZIP_HEADER, head_stream, tail.deflated(), trailer))
