from __future__ import annotations

from copy import deepcopy
import hashlib
import json

import pytest

from boatrace_ai.pipelines import pre_night_program_binding as binding


RACE_DATE = "2026-08-10"

DEADLINE_01 = "1" * 64
DEADLINE_02 = "2" * 64
PROGRAM_01 = "a" * 64
PROGRAM_02 = "b" * 64
COLLECTION_SHA256 = "c" * 64


def _canonical(value) -> bytes:
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


def _program_entries(
    venue_codes=("01", "02"),
) -> list[dict]:
    return [
        {
            "race_date": RACE_DATE,
            "venue_code": venue_code,
            "race_no": race_no,
            "boat_no": boat_no,
        }
        for venue_code in venue_codes
        for race_no in range(1, 13)
        for boat_no in range(1, 7)
    ]


def _build(
    *,
    deadline_by_venue=None,
    program_by_venue=None,
    entries=None,
):
    return binding.build_pre_night_program_entries_binding(
        race_date=RACE_DATE,
        deadline_evidence_collection_sha256=(
            COLLECTION_SHA256
        ),
        deadline_evidence_sha256_by_venue=(
            {
                "01": DEADLINE_01,
                "02": DEADLINE_02,
            }
            if deadline_by_venue is None
            else deadline_by_venue
        ),
        program_source_sha256_by_venue=(
            {
                "01": PROGRAM_01,
                "02": PROGRAM_02,
            }
            if program_by_venue is None
            else program_by_venue
        ),
        program_entries=(
            _program_entries()
            if entries is None
            else entries
        ),
    )


def _expected_venue_digest(
    payload: dict,
    venue_code: str,
) -> str:
    venue = payload["venue_bindings"][venue_code]

    material = {
        "race_date": payload["race_date"],
        "venue_code": venue_code,
        "deadline_evidence_sha256": (
            venue["deadline_evidence_sha256"]
        ),
        "program_source_sha256": (
            venue["program_source_sha256"]
        ),
        "races": venue["races"],
    }

    return hashlib.sha256(
        _canonical(material)
    ).hexdigest()


def test_each_venue_digest_binds_venue_identity_and_inputs():
    payload = _build()

    assert list(payload["venue_bindings"]) == [
        "01",
        "02",
    ]

    for venue_code in ("01", "02"):
        assert (
            payload["venue_bindings"][venue_code][
                "binding_sha256"
            ]
            == _expected_venue_digest(
                payload,
                venue_code,
            )
        )


def test_venue_binding_contains_complete_race_boat_grid():
    payload = _build()

    expected_races = {
        str(race_no)
        for race_no in range(1, 13)
    }
    expected_boats = {
        str(boat_no)
        for boat_no in range(1, 7)
    }

    for venue_code in ("01", "02"):
        races = payload[
            "venue_bindings"
        ][venue_code]["races"]

        assert set(races) == expected_races

        for race_no in expected_races:
            assert set(races[race_no]) == {"boats"}
            assert (
                set(races[race_no]["boats"])
                == expected_boats
            )
            assert all(
                boat == {}
                for boat in races[
                    race_no
                ]["boats"].values()
            )


def test_input_order_does_not_change_binding():
    forward = _build()

    reversed_input = _build(
        deadline_by_venue={
            "02": DEADLINE_02,
            "01": DEADLINE_01,
        },
        program_by_venue={
            "02": PROGRAM_02,
            "01": PROGRAM_01,
        },
        entries=list(
            reversed(_program_entries())
        ),
    )

    assert reversed_input == forward

    assert (
        binding
        .canonical_program_entries_binding_bytes(
            reversed_input
        )
        ==
        binding
        .canonical_program_entries_binding_bytes(
            forward
        )
    )


def test_program_digest_change_is_isolated_to_one_venue():
    original = _build()

    changed = _build(
        program_by_venue={
            "01": "d" * 64,
            "02": PROGRAM_02,
        }
    )

    assert (
        changed["venue_bindings"]["01"][
            "binding_sha256"
        ]
        != original["venue_bindings"]["01"][
            "binding_sha256"
        ]
    )

    assert (
        changed["venue_bindings"]["02"]
        == original["venue_bindings"]["02"]
    )


def test_deadline_digest_change_is_isolated_to_one_venue():
    original = _build()

    changed = _build(
        deadline_by_venue={
            "01": "e" * 64,
            "02": DEADLINE_02,
        }
    )

    assert (
        changed["venue_bindings"]["01"][
            "binding_sha256"
        ]
        != original["venue_bindings"]["01"][
            "binding_sha256"
        ]
    )

    assert (
        changed["venue_bindings"]["02"]
        == original["venue_bindings"]["02"]
    )


@pytest.mark.parametrize(
    "program_by_venue",
    [
        {
            "01": PROGRAM_01,
        },
        {
            "01": PROGRAM_01,
            "02": PROGRAM_02,
            "03": "3" * 64,
        },
    ],
)
def test_program_digest_coverage_must_match_deadline_venues(
    program_by_venue,
):
    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="program source venue coverage mismatch",
    ):
        _build(
            program_by_venue=program_by_venue,
        )


def test_missing_venue_program_rows_fail_closed():
    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="must contain races 1 through 12",
    ):
        _build(
            entries=_program_entries(
                venue_codes=("01",)
            )
        )


def test_missing_boat_identity_fails_closed():
    entries = [
        row
        for row in _program_entries()
        if not (
            row["venue_code"] == "02"
            and row["race_no"] == 12
            and row["boat_no"] == 6
        )
    ]

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="must contain boats 1 through 6",
    ):
        _build(entries=entries)


def test_duplicate_program_identity_fails_closed():
    entries = _program_entries()
    entries.append(dict(entries[0]))

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="duplicate",
    ):
        _build(entries=entries)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        (
            "deadline_evidence_sha256",
            "f" * 64,
        ),
        (
            "program_source_sha256",
            "0" * 64,
        ),
    ],
)
def test_tampered_venue_input_is_rejected(
    field_name,
    replacement,
):
    payload = deepcopy(_build())

    payload["venue_bindings"]["01"][
        field_name
    ] = replacement

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="venue binding SHA-256 mismatch",
    ):
        binding.canonical_program_entries_binding_bytes(
            payload
        )


def test_tampered_race_grid_is_rejected():
    payload = deepcopy(_build())

    del payload["venue_bindings"]["01"][
        "races"
    ]["12"]["boats"]["6"]

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="boat keys mismatch",
    ):
        binding.canonical_program_entries_binding_bytes(
            payload
        )


def test_digest_from_one_venue_cannot_be_reused_by_another():
    payload = deepcopy(_build())

    payload["venue_bindings"]["02"][
        "binding_sha256"
    ] = payload["venue_bindings"]["01"][
        "binding_sha256"
    ]

    with pytest.raises(
        binding.PreNightProgramBindingContractError,
        match="venue binding SHA-256 mismatch: 02",
    ):
        binding.canonical_program_entries_binding_bytes(
            payload
        )
