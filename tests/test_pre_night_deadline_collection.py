import hashlib
import json

import pytest

from boatrace_ai.pipelines import pre_night_deadline_collection as collection


def _install_contract(monkeypatch):
    def validate(value):
        if not isinstance(value, dict):
            raise ValueError("invalid evidence")
        return dict(value)

    def canonical(value):
        return (
            json.dumps(
                validate(value),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    monkeypatch.setattr(
        collection,
        "validate_deadline_evidence",
        validate,
    )
    monkeypatch.setattr(
        collection,
        "canonical_deadline_evidence_bytes",
        canonical,
    )

    return canonical


def _stage1_path(root, race_date, venue_code):
    year, month, day = race_date.split("-")
    return (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence"
        / year
        / month
        / day
        / venue_code
        / "deadline_evidence.json"
    )


def _write_stage1(
    root,
    canonical,
    venues=("01", "02"),
    race_date="2026-07-30",
):
    paths = {}

    for venue_code in venues:
        payload = {
            "contract_version": "test-stage1-v1",
            "race_date": race_date,
            "venue_code": venue_code,
            "race_deadlines": [],
        }
        path = _stage1_path(
            root,
            race_date,
            venue_code,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(payload))
        paths[venue_code] = path

    return paths


def _collect(root, venues=("01", "02")):
    return collection.collect_pre_night_deadline_evidence(
        root,
        race_date="2026-07-30",
        expected_venue_codes=list(venues),
    )


def test_s2_creates_deterministic_collection(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    result = _collect(
        tmp_path,
        venues=("02", "01"),
    )
    path = result["paths"][
        "deadline_evidence_collection"
    ]
    payload = json.loads(path.read_text("utf-8"))

    assert result["publication_status"] == "CREATED"
    assert result["cached"] is False
    assert payload["expected_venue_codes"] == ["01", "02"]
    assert [
        entry["venue_code"]
        for entry in payload["entries"]
    ] == ["01", "02"]
    assert payload["entry_count"] == 2
    assert "collection_sha256" not in payload
    assert "generated_at" not in payload


def test_s2_exact_digest_and_length(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    result = _collect(tmp_path)
    stored = result["paths"][
        "deadline_evidence_collection"
    ].read_bytes()

    assert result[
        "deadline_evidence_collection_sha256"
    ] == hashlib.sha256(stored).hexdigest()
    assert result["byte_length"] == len(stored)
    assert stored.endswith(b"\n")
    assert not stored.endswith(b"\n\n")


def test_s2_validated_cache_reuse(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    first = _collect(tmp_path)
    before = first["paths"][
        "deadline_evidence_collection"
    ].stat().st_mtime_ns
    second = _collect(tmp_path)
    after = second["paths"][
        "deadline_evidence_collection"
    ].stat().st_mtime_ns

    assert second["cached"] is True
    assert second["publication_status"] == "VALIDATED_REUSE"
    assert before == after


@pytest.mark.parametrize(
    "values",
    [
        [],
        ["00"],
        ["25"],
        ["1"],
        ["01", "01"],
        "01",
    ],
)
def test_s2_rejects_invalid_expected_venues(
    tmp_path,
    monkeypatch,
    values,
):
    _install_contract(monkeypatch)

    with pytest.raises(
        collection.PreNightDeadlineCollectionContractError
    ):
        collection.collect_pre_night_deadline_evidence(
            tmp_path,
            race_date="2026-07-30",
            expected_venue_codes=values,
        )


def test_s2_rejects_missing_venue(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(
        tmp_path,
        canonical,
        venues=("01",),
    )

    with pytest.raises(
        collection.PreNightDeadlineCollectionCacheError,
        match="missing",
    ):
        _collect(tmp_path)


def test_s2_rejects_extra_venue(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(
        tmp_path,
        canonical,
        venues=("01", "02", "03"),
    )

    with pytest.raises(
        collection.PreNightDeadlineCollectionCacheError,
        match="extra",
    ):
        _collect(tmp_path)


def test_s2_rejects_noncanonical_stage1(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    paths = _write_stage1(tmp_path, canonical)

    payload = json.loads(
        paths["01"].read_text("utf-8")
    )
    paths["01"].write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        collection.PreNightDeadlineCollectionCacheError,
        match="non-canonical",
    ):
        _collect(tmp_path)


def test_s2_rejects_malformed_stage1(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    paths = _write_stage1(tmp_path, canonical)
    paths["01"].write_text("{bad", encoding="utf-8")

    with pytest.raises(
        collection.PreNightDeadlineCollectionCacheError,
        match="valid UTF-8 JSON",
    ):
        _collect(tmp_path)


def test_s2_rejects_stage1_identity_mismatch(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    paths = _write_stage1(tmp_path, canonical)
    payload = json.loads(paths["01"].read_text("utf-8"))
    payload["venue_code"] = "03"
    paths["01"].write_bytes(canonical(payload))

    with pytest.raises(
        collection.PreNightDeadlineCollectionCacheError,
        match="venue_code mismatch",
    ):
        _collect(tmp_path)


def test_s2_unique_temporary_paths(tmp_path):
    first = collection._collection_paths(
        tmp_path,
        "2026-07-30",
    )
    second = collection._collection_paths(
        tmp_path,
        "2026-07-30",
    )

    assert first["temporary"] != second["temporary"]
    assert first["temporary"].parent == first["directory"]
    assert first["temporary"].name.endswith(".tmp")


def test_s2_success_cleans_temp_and_lock(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    result = _collect(tmp_path)
    directory = result["paths"]["directory"]

    assert not list(
        directory.glob(
            ".deadline_evidence_collection.json.*.tmp"
        )
    )
    assert not (
        directory / ".deadline_evidence_collection.lock"
    ).exists()


def test_s2_existing_lock_is_preserved(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    paths = collection._collection_paths(
        tmp_path,
        "2026-07-30",
    )
    paths["directory"].mkdir(parents=True)
    paths["lock"].write_text("foreign", encoding="utf-8")

    with pytest.raises(
        collection.PreNightDeadlineCollectionError,
        match="lock exists",
    ):
        _collect(tmp_path)

    assert paths["lock"].read_text("utf-8") == "foreign"


def test_s2_parent_directory_fsync_is_attempted(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)
    calls = []
    original = collection._fsync_directory

    def counted(path):
        calls.append(path)
        return original(path)

    monkeypatch.setattr(
        collection,
        "_fsync_directory",
        counted,
    )

    result = _collect(tmp_path)

    assert calls == [result["paths"]["directory"]]


def test_s2_directory_fsync_failure_is_fail_closed(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    def fail(_path):
        raise collection.PreNightDeadlineCollectionIntegrityError(
            "injected directory fsync failure"
        )

    monkeypatch.setattr(
        collection,
        "_fsync_directory",
        fail,
    )

    with pytest.raises(
        collection.PreNightDeadlineCollectionIntegrityError,
        match="injected directory fsync failure",
    ):
        _collect(tmp_path)


def test_s2_atomic_conflict_does_not_overwrite(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)

    def competing_link(source, destination):
        destination.write_bytes(source.read_bytes())
        raise FileExistsError("competing writer")

    monkeypatch.setattr(
        collection.os,
        "link",
        competing_link,
    )

    result = _collect(tmp_path)

    assert result["cached"] is True
    assert result["publication_status"] == "VALIDATED_REUSE"


def test_s2_symlink_root_fails_closed(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"

    try:
        linked.symlink_to(actual, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        collection.PreNightDeadlineCollectionContractError,
        match="symlink",
    ):
        collection.collect_pre_night_deadline_evidence(
            linked,
            race_date="2026-07-30",
            expected_venue_codes=["01"],
        )


def test_s2_stage1_venue_symlink_fails_closed(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(
        tmp_path,
        canonical,
        venues=("01",),
    )

    date_directory = _stage1_path(
        tmp_path,
        "2026-07-30",
        "01",
    ).parent.parent
    actual = date_directory / "02-actual"
    actual.mkdir()
    linked = date_directory / "02"

    try:
        linked.symlink_to(actual, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        collection.PreNightDeadlineCollectionContractError,
        match="symlink",
    ):
        _collect(tmp_path)


def test_s2_does_not_create_later_stage_artifacts(
    tmp_path,
    monkeypatch,
):
    canonical = _install_contract(monkeypatch)
    _write_stage1(tmp_path, canonical)
    _collect(tmp_path)

    names = {
        path.name
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert "deadline_evidence_collection.json" in names
    assert "program_entries_binding.json" not in names
    assert "pipeline_manifest.json" not in names
    assert "execution_manifest.json" not in names
    assert "prospective_manifest.json" not in names
    assert "snapshot.parquet" not in names


def test_s2_rejects_duplicate_expected_venue_codes(
    tmp_path,
    monkeypatch,
):
    _install_contract(monkeypatch)

    with pytest.raises(
        collection.PreNightDeadlineCollectionContractError,
        match="contains duplicate",
    ):
        _collect(
            tmp_path,
            venues=("01", "01"),
        )
