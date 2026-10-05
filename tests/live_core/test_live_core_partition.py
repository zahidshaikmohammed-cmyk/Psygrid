"""Partitioning of the canonical 989-stock universe across Live Core nodes, and node configuration."""

import json
from pathlib import Path

import pytest

import config as psygrid_config
from live_core.config import LiveCoreConfig, LiveCoreConfigError, parse_peers
from live_core.partition import (
    PartitionError,
    all_partitions,
    build_partition,
    load_universe,
    make_universe,
    nodes_for_range,
    owner_of_index,
    partition_bounds,
    shard_ranges,
    validate_partitions,
)

ROOT = Path(__file__).resolve().parents[2]


def _stocks_json_symbols():
    return json.loads((ROOT / "stocks.json").read_text(encoding="utf-8"))["symbols"]


def test_universe_is_the_unmodified_canonical_stocks_json(universe):
    assert universe.size == 989 == psygrid_config.UNIVERSE_SIZE
    assert list(universe.symbols) == [symbol.strip().upper() for symbol in _stocks_json_symbols()]
    assert list(universe.symbols) == psygrid_config._load_symbol_universe()


def test_partition_is_deterministic_and_reproducible(universe):
    first = [build_partition(universe, node, 2) for node in range(2)]
    second = [build_partition(load_universe(), node, 2) for node in range(2)]
    assert first == second
    assert [p.fingerprint for p in first] == [p.fingerprint for p in second]
    # Fingerprints are content-derived constants, so two hosts on the same commit agree.
    assert first[0].universe_fingerprint == first[1].universe_fingerprint == universe.fingerprint
    assert first[0].fingerprint != first[1].fingerprint


def test_node_0_gets_495_and_node_1_gets_494(universe):
    node0, node1 = all_partitions(universe, 2)
    assert (node0.start, node0.end, node0.size) == (0, 495, 495)
    assert (node1.start, node1.end, node1.size) == (495, 989, 494)
    assert node0.symbols == universe.symbols[:495]
    assert node1.symbols == universe.symbols[495:]


def test_zero_overlap_between_nodes(universe):
    node0, node1 = all_partitions(universe, 2)
    assert set(node0.symbols).isdisjoint(node1.symbols)


def test_union_is_the_complete_universe_exactly_once(universe):
    partitions = validate_partitions(universe, 2)
    flattened = [symbol for partition in partitions for symbol in partition.symbols]
    assert len(flattened) == 989
    assert len(set(flattened)) == 989
    assert tuple(flattened) == universe.symbols
    assert set(flattened) == set(_stocks_json_symbols())


@pytest.mark.parametrize("node_count", [1, 2, 3, 4, 5, 7, 8])
def test_any_node_count_covers_every_stock_once_with_balanced_sizes(universe, node_count):
    partitions = validate_partitions(universe, node_count)
    sizes = [p.size for p in partitions]
    assert sum(sizes) == 989
    assert max(sizes) - min(sizes) <= 1
    for index in range(universe.size):
        owners = [p.node_id for p in partitions if p.owns_index(index)]
        assert owners == [owner_of_index(universe.size, node_count, index)]


def test_existing_shards_map_wholly_onto_one_node(universe):
    shards = shard_ranges(universe.size)
    assert [name for name, _, _ in shards] == list("abcdefghijklmnopqrstuv")
    owners = {}
    for name, start, end in shards:
        pieces = nodes_for_range(universe.size, 2, start, end)
        assert len(pieces) == 1, f"shard {name} straddles nodes: {pieces}"
        assert pieces[0][1:] == (start, end)
        owners[name] = pieces[0][0]
    assert {name for name, owner in owners.items() if owner == 0} == set("abcdefghijk")
    assert {name for name, owner in owners.items() if owner == 1} == set("lmnopqrstuv")


def test_shard_ranges_match_the_full_app_definition(universe):
    expected = tuple((name, i * 45, min((i + 1) * 45, 989)) for i, name in enumerate("abcdefghijklmnopqrstuv"))
    assert shard_ranges(universe.size) == expected


def test_full_range_splits_into_both_partitions(universe):
    assert nodes_for_range(989, 2, 0, 989) == [(0, 0, 495), (1, 495, 989)]
    assert nodes_for_range(989, 2, 490, 500) == [(0, 490, 495), (1, 495, 500)]


def test_invalid_universes_and_nodes_are_rejected():
    with pytest.raises(PartitionError):
        make_universe(["AAA", "aaa"])
    with pytest.raises(PartitionError):
        make_universe([])
    with pytest.raises(PartitionError):
        make_universe(["AAA", "B\tB"])
    with pytest.raises(PartitionError):
        partition_bounds(989, 2, 2)
    with pytest.raises(PartitionError):
        partition_bounds(989, -1, 2)
    with pytest.raises(PartitionError):
        partition_bounds(1, 0, 2)


def test_a_wrong_sized_stocks_file_refuses_to_load(tmp_path, monkeypatch):
    path = tmp_path / "stocks.json"
    symbols = _stocks_json_symbols()[:-1]
    path.write_text(json.dumps({"exchange": "NSE", "instrument": "EQUITY", "symbols": symbols}), encoding="utf-8")
    monkeypatch.setenv("PSYGRID_STOCKS_FILE", str(path))
    with pytest.raises(PartitionError, match="989"):
        load_universe()


def test_config_reads_node_identity_and_peers():
    cfg = LiveCoreConfig.from_environment(
        {"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "2", "LIVE_CORE_PEERS": "1=http://10.0.0.12:10000/"}
    )
    assert (cfg.node_id, cfg.node_count, cfg.port) == (0, 2, 10000)
    assert cfg.peers == {1: "http://10.0.0.12:10000"}
    assert cfg.missing_peers() == []
    node1 = LiveCoreConfig.from_environment({"LIVE_CORE_NODE_ID": "1", "LIVE_CORE_NODE_COUNT": "2"})
    assert node1.missing_peers() == [0]


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {"LIVE_CORE_NODE_ID": "0"},
        {"LIVE_CORE_NODE_ID": "2", "LIVE_CORE_NODE_COUNT": "2"},
        {"LIVE_CORE_NODE_ID": "x", "LIVE_CORE_NODE_COUNT": "2"},
        {"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "0"},
        {"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "2", "LIVE_CORE_PORT": "70000"},
        {"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "2", "LIVE_CORE_PEER_TIMEOUT_SECONDS": "-1"},
    ],
)
def test_invalid_node_configuration_is_rejected(environ):
    with pytest.raises(LiveCoreConfigError):
        LiveCoreConfig.from_environment(environ)


@pytest.mark.parametrize(
    "raw",
    ["0=http://a:1", "1=ftp://a:1", "1", "3=http://a:1", "1=http://a:1,1=http://b:1", "x=http://a:1"],
)
def test_invalid_peer_lists_are_rejected(raw):
    with pytest.raises(LiveCoreConfigError):
        parse_peers(raw, node_id=0, node_count=2)
