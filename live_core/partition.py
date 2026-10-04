"""Deterministic partition of the canonical stocks.json universe across Live Core nodes.

Node ``i`` of ``K`` owns the contiguous canonical-order block ``[ceil(i*N/K), ceil((i+1)*N/K))``.
For the 989-stock universe and two nodes that is ``[0, 495)`` and ``[495, 989)``: 495 and 494
stocks. Because 495 = 11 * 45, every one of the existing 45-stock shards ``live-a`` .. ``live-v``
lies wholly on one node (a..k on node 0, l..v on node 1), so a shard never needs merging.

Nothing here is a hard-coded stock list: both nodes derive their block from the same file and
publish fingerprints so a node can refuse to merge with a peer built from a different universe.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import config as psygrid_config

SHARD_SIZE = 45
SHARD_NAMES = "abcdefghijklmnopqrstuv"


class PartitionError(RuntimeError):
    """The canonical universe or its partition failed validation."""


@dataclass(frozen=True)
class Universe:
    symbols: tuple[str, ...]
    fingerprint: str

    @property
    def size(self) -> int:
        return len(self.symbols)

    def index_of(self, symbol: str) -> int | None:
        return _index_map(self).get(symbol.strip().upper())


_INDEX_CACHE: dict[str, dict[str, int]] = {}


def _index_map(universe: Universe) -> dict[str, int]:
    cached = _INDEX_CACHE.get(universe.fingerprint)
    if cached is None:
        cached = {symbol: index for index, symbol in enumerate(universe.symbols)}
        _INDEX_CACHE[universe.fingerprint] = cached
    return cached


def universe_fingerprint(symbols) -> str:
    return hashlib.sha256("\n".join(symbols).encode("utf-8")).hexdigest()


def make_universe(symbols) -> Universe:
    normalized = tuple(str(symbol).strip().upper() for symbol in symbols)
    if not normalized:
        raise PartitionError("canonical universe is empty")
    if any(not symbol for symbol in normalized):
        raise PartitionError("canonical universe contains an empty symbol")
    if len(set(normalized)) != len(normalized):
        raise PartitionError("canonical universe contains duplicate symbols")
    if any(char in symbol for symbol in normalized for char in "\t\n\r"):
        raise PartitionError("canonical universe contains a symbol with a control character")
    return Universe(normalized, universe_fingerprint(normalized))


def load_universe() -> Universe:
    """Load and validate ``stocks.json`` exactly as the full PSYGRID does (989 unique NSE equities)."""
    try:
        symbols = psygrid_config._load_symbol_universe()
    except RuntimeError as exc:
        raise PartitionError(str(exc)) from exc
    universe = make_universe(symbols)
    if universe.size != psygrid_config.UNIVERSE_SIZE:
        raise PartitionError(f"canonical universe has {universe.size} symbols; expected {psygrid_config.UNIVERSE_SIZE}")
    return universe


def partition_bounds(size: int, node_id: int, node_count: int) -> tuple[int, int]:
    if node_count < 1 or not 0 <= node_id < node_count:
        raise PartitionError(f"invalid node {node_id} of {node_count}")
    if size < node_count:
        raise PartitionError(f"cannot split {size} instruments across {node_count} nodes")
    start = -((-node_id * size) // node_count)
    end = -((-(node_id + 1) * size) // node_count)
    return start, end


def owner_of_index(size: int, node_count: int, index: int) -> int:
    if not 0 <= index < size:
        raise PartitionError(f"index {index} outside the universe of {size}")
    return (index * node_count) // size


@dataclass(frozen=True)
class Partition:
    node_id: int
    node_count: int
    start: int
    end: int
    symbols: tuple[str, ...]
    universe_fingerprint: str
    fingerprint: str

    @property
    def size(self) -> int:
        return self.end - self.start

    def owns_index(self, index: int) -> bool:
        return self.start <= index < self.end

    def describe(self) -> dict:
        return {
            "node_id": self.node_id,
            "node_count": self.node_count,
            "start_index": self.start,
            "end_index": self.end,
            "expected_instrument_count": self.size,
            "first_symbol": self.symbols[0] if self.symbols else None,
            "last_symbol": self.symbols[-1] if self.symbols else None,
            "fingerprint": self.fingerprint,
            "universe_fingerprint": self.universe_fingerprint,
        }


def build_partition(universe: Universe, node_id: int, node_count: int) -> Partition:
    start, end = partition_bounds(universe.size, node_id, node_count)
    fingerprint = hashlib.sha256(
        f"{universe.fingerprint}:{node_count}:{node_id}:{start}:{end}".encode("ascii")
    ).hexdigest()
    return Partition(node_id, node_count, start, end, universe.symbols[start:end], universe.fingerprint, fingerprint)


def all_partitions(universe: Universe, node_count: int) -> list[Partition]:
    return [build_partition(universe, node_id, node_count) for node_id in range(node_count)]


def validate_partitions(universe: Universe, node_count: int) -> list[Partition]:
    """Prove that the partitions cover the universe exactly once; raise ``PartitionError`` otherwise."""
    partitions = all_partitions(universe, node_count)
    flattened = [symbol for partition in partitions for symbol in partition.symbols]
    if len(flattened) != universe.size:
        raise PartitionError(f"partitions hold {len(flattened)} instruments; universe has {universe.size}")
    if len(set(flattened)) != len(flattened):
        raise PartitionError("an instrument is assigned to more than one node")
    if tuple(flattened) != universe.symbols:
        raise PartitionError("partitions do not reproduce the canonical universe order")
    sizes = [partition.size for partition in partitions]
    if min(sizes) < 1 or max(sizes) - min(sizes) > 1:
        raise PartitionError(f"unbalanced partition sizes: {sizes}")
    for index in range(universe.size):
        owner = owner_of_index(universe.size, node_count, index)
        if not partitions[owner].owns_index(index):
            raise PartitionError(f"index {index} owner mismatch")
    return partitions


def shard_ranges(size: int) -> tuple[tuple[str, int, int], ...]:
    """The existing ``live-a`` .. ``live-v`` family: canonical-order slices of 45 (same rule as app.py)."""
    return tuple(
        (name, index * SHARD_SIZE, min((index + 1) * SHARD_SIZE, size)) for index, name in enumerate(SHARD_NAMES)
    )


def nodes_for_range(size: int, node_count: int, start: int, end: int) -> list[tuple[int, int, int]]:
    """Split the canonical range ``[start, end)`` into ``(node_id, sub_start, sub_end)`` pieces."""
    start = max(0, start)
    end = min(size, end)
    pieces = []
    for node_id in range(node_count):
        node_start, node_end = partition_bounds(size, node_id, node_count)
        sub_start, sub_end = max(start, node_start), min(end, node_end)
        if sub_start < sub_end:
            pieces.append((node_id, sub_start, sub_end))
    return pieces
