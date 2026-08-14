from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

from boatrace_ai.pipelines import (
    pre_night_program_binding as binding,
)
from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)


RACE_DATE = "2026-07-30"
RUN_ID = "pre-night-20260729T120000Z-test"
D1 = "1" * 64
D2 = "2" * 64
P1 = "a" * 64
P2 = "b" * 64


def _canonical(value):
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _collection_path(root):
    return (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence_collections"
        / "2026"
        / "07"
        / "30"
        / "deadline_evidence_collection.json"
    )


def _write_collection(root):
    entries = [
        {
            "race_date": RACE_DATE,
            "venue_code": "01",
            "relative_path": (
                "prospective/pre_night/deadline_evidence/"
                "2026/07/30/01/deadline_evidence.json"
            ),
            "deadline_evidence_sha256": D1,
            "byte_length": 100,
            "contract_version": "test-stage1-v1",
        },
        {
            "race_date": RACE_DATE,
            "venue_code": "02",
            "relative_path": (
                "prospective/pre_night/deadline_evidence/"
                "2026/07/30/02/deadline_evidence.json"
            ),
            "deadline_evidence_sha256": D2,
            "byte_length": 101,
            "contract_version": "test-stage1-v1",
        },
    ]
    payload = {
        "contract_version": COLLECTION_CONTRACT_VERSION,
        "race_date": RACE_DATE,
        "expected_venue_codes": ["01", "02"],
        "entry_count": 2,
        "entries": entries,
    }
    stored = _canonical(payload)
    path = _collection_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(stored)
    return path, hashlib.sha256(stored).hexdigest()


def _program_entries(venues=("01", "02")):
    return [
        {
            "race_date": RACE_DATE,
            "venue_code": venue,
            "race_no": race_no,
            "boat_no": boat_no,
            "racer_id": (
                f"{int(venue):02d}{race_no:02d}"
            ),
        }
        for venue in venues
        for race_no in range(1, 13)
        for boat_no in range(1, 7)
    ]


def _publish(root, *, entries=None, sources=None):
    _, digest = _write_collection(root)
    return binding.publish_pre_night_program_entries_binding(
        root,
        run_id=RUN_ID,
        race_date=RACE_DATE,
        deadline_evidence_collection_sha256=digest,
        program_source_sha256_by_venue=(
            {"01": P1, "02": P2}
            if sources is None
            else sources
        ),
        program_entries=(
            _program_entries()
            if entries is None
            else entries
        ),
    )


def test_s3_creates_exact_binding():
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as directory:
        root = Path(directory)
        result = _publish(root)
        path = result["paths"]["program_entries_binding"]
        stored = path.read_bytes()
        payload = json.loads(stored.decode("utf-8"))

        assert result["publication_status"] == "CREATED"
        assert result["cached"] is False
        assert payload["contract_version"] == (
            binding.PROGRAM_BINDING_CONTRACT_VERSION
        )
        assert list(payload["venue_bindings"]) == ["01", "02"]

        races = payload["venue_bindings"]["01"]["races"]
        assert isinstance(races, dict)
        assert set(races) == {
            str(race_no)
            for race_no in range(1, 13)
        }

        boats = races["1"]["boats"]
        assert isinstance(boats, dict)
        assert set(boats) == {
            str(boat_no)
            for boat_no in range(1, 7)
        }
        assert all(
            boat_binding == {}
            for boat_binding in boats.values()
        )

        assert "overall_binding_sha256" not in payload
        assert "generated_at" not in payload
        assert stored == _canonical(payload)


def test_s3_exact_overall_digest_and_length(tmp_path):
    result = _publish(tmp_path)
    stored = result["paths"][
        "program_entries_binding"
    ].read_bytes()

    expected = hashlib.sha256(stored).hexdigest()

    assert result["program_entries_binding_sha256"] == expected
    assert result["overall_binding_sha256"] == expected
    assert result["byte_length"] == len(stored)
    assert stored.endswith(b"\n")
    assert not stored.endswith(b"\n\n")


def test_s3_venue_binding_digest_is_exact(tmp_path):
    result = _publish(tmp_path)
    payload = json.loads(
        result["paths"]["program_entries_binding"].read_text(
            encoding="utf-8"
        )
    )

    venue = payload["venue_bindings"]["01"]
    material = {
        "race_date": RACE_DATE,
        "venue_code": "01",
        "deadline_evidence_sha256": D1,
        "program_source_sha256": P1,
        "races": venue["races"],
    }

    assert venue["binding_sha256"] == hashlib.sha256(
        _canonical(material)
    ).hexdigest()


def test_s3_program_rows_are_deterministic(tmp_path):
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"

    first = _publish(
        first_root,
        entries=_program_entries(),
        sources={"02": P2, "01": P1},
    )
    second = _publish(
        second_root,
        entries=list(reversed(_program_entries())),
        sources={"01": P1, "02": P2},
    )

    assert (
        first["paths"]["program_entries_binding"].read_bytes()
        == second["paths"]["program_entries_binding"].read_bytes()
    )


def test_s3_validated_cache_reuse(tmp_path):
    first = _publish(tmp_path)
    path = first["paths"]["program_entries_binding"]
    before = path.stat().st_mtime_ns

    second = _publish(tmp_path)
    after = path.stat().st_mtime_ns

    assert second["cached"] is True
    assert second["publication_status"] == "VALIDATED_REUSE"
    assert before == after


def test_s3_run_relative_path(tmp_path):
    result = _publish(tmp_path)

    assert result["relative_path"] == (
        "prospective/pre_night/runs/"
        "2026/07/30/"
        f"{RUN_ID}/program_entries_binding.json"
    )


def test_s3_rejects_collection_digest_mismatch(tmp_path):
    _write_collection(tmp_path)

    with pytest.raises(
        binding.PreNightProgramBindingCacheError,
        match="SHA-256 mismatch",
    ):
        binding.publish_pre_night_program_entries_binding(
            tmp_path,
            run_id=RUN_ID,
            race_date=RACE_DATE,
            deadline_evidence_collection_sha256="0" * 64,
            program_source_sha256_by_venue={
                "01": P1,
                "02": P2,
            },
            program_entries=_program_entries(),
        )


@pytest.mark.parametrize(
    "sources",
    [
        {"01": P1},
        {"01": P1, "02": P2, "03": "c" * 64},
        {"01": P1, "02": "A" * 64},
    ],
)
def test_s3_rejects_invalid_program_source_mapping(
    tmp_path,
    sources,
):
    with pytest.raises(
        binding.PreNightProgramBindingContractError
    ):
        _publish(tmp_path, sources=sources)


def test_s3_rejects_duplicate_program_boat(tmp_path):
    entries = _program_entries()
    entries.append(dict(entries[0]))

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="duplicate",
    ):
        _publish(tmp_path, entries=entries)


def test_s3_rejects_missing_boat(tmp_path):
    entries = _program_entries()
    entries = [
        row
        for row in entries
        if not (
            row["venue_code"] == "01"
            and row["race_no"] == 1
            and row["boat_no"] == 6
        )
    ]

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="boats 1 through 6",
    ):
        _publish(tmp_path, entries=entries)


def test_s3_rejects_missing_race(tmp_path):
    entries = [
        row
        for row in _program_entries()
        if not (
            row["venue_code"] == "02"
            and row["race_no"] == 12
        )
    ]

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="races 1 through 12",
    ):
        _publish(tmp_path, entries=entries)


def test_s3_rejects_program_identity_mismatch(tmp_path):
    entries = _program_entries()
    entries[0] = {
        **entries[0],
        "race_date": "2026-07-31",
    }

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="race_date mismatch",
    ):
        _publish(tmp_path, entries=entries)


def test_s3_rejects_conflicting_cache(tmp_path):
    first = _publish(tmp_path)
    destination = first["paths"][
        "program_entries_binding"
    ]
    destination.write_text(
        '{"conflict":true}\n',
        encoding="utf-8",
    )

    with pytest.raises(
        binding.PreNightProgramBindingCacheError
    ):
        _publish(tmp_path)


def test_s3_symlink_root_fails_closed(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"

    try:
        linked.symlink_to(
            actual,
            target_is_directory=True,
        )
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="symlink",
    ):
        _publish(linked)


def test_s3_success_cleans_owned_temp_and_lock(tmp_path):
    result = _publish(tmp_path)
    directory = result["paths"]["directory"]

    assert not (
        directory / ".program_entries_binding.lock"
    ).exists()
    assert not list(
        directory.glob(
            ".program_entries_binding.json.*.tmp"
        )
    )


def test_s3_does_not_publish_later_stage_artifacts(tmp_path):
    _publish(tmp_path)

    names = {
        path.name
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert "program_entries_binding.json" in names
    assert "pipeline_manifest.json" not in names
    assert "execution_manifest.json" not in names
    assert "prospective_manifest.json" not in names
    assert "feature_matrix.parquet" not in names


def test_s3_uses_link_and_not_replace():
    tree = ast.parse(
        Path(binding.__file__).read_text(
            encoding="utf-8"
        )
    )

    calls = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if isinstance(node.func, ast.Attribute):
            calls.add(node.func.attr)
        elif isinstance(node.func, ast.Name):
            calls.add(node.func.id)

    assert "link" in calls
    assert "replace" not in calls


def test_s3_rejects_program_entry_venue_not_in_deadline_collection(
    tmp_path,
):
    entries = _program_entries()
    entries[0] = {
        **entries[0],
        "venue_code": "03",
    }

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="program entry venue is not in deadline collection",
    ):
        _publish(tmp_path, entries=entries)
