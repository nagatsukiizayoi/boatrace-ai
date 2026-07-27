from __future__ import annotations

import json

import pytest

from boatrace_ai.pipelines.pre_night_eligibility import (
    PreNightEligibilityDecision,
    PreNightEligibilityStatus,
    classify_pre_night_exception,
    decision_from_exception,
    eligible_decision,
    manifest_eligibility_fields,
    skipped_decision,
)


RACE_DATE = "2026-07-27"
AS_OF_TIME = "2026-07-26T21:30:00+09:00"


def test_eligible_decision_is_exact():
    decision = eligible_decision(
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is PreNightEligibilityStatus.ELIGIBLE
    assert decision.eligible is True
    assert decision.race_date == RACE_DATE
    assert decision.as_of_time == AS_OF_TIME


def test_program_unavailable_status():
    decision = decision_from_exception(
        FileNotFoundError("program unavailable"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_PROGRAM_UNAVAILABLE
    )
    assert decision.eligible is False


def test_provenance_missing_status():
    decision = decision_from_exception(
        ValueError("provenance missing"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_PROVENANCE_MISSING
    )


def test_fetched_after_as_of_status():
    decision = decision_from_exception(
        ValueError("fetched after as_of time"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_FETCHED_AFTER_AS_OF
    )


def test_hash_mismatch_status():
    decision = decision_from_exception(
        ValueError("response sha256 mismatch"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_HASH_MISMATCH
    )


def test_metadata_invalid_status():
    decision = decision_from_exception(
        ValueError("metadata invalid: timezone required"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_METADATA_INVALID
    )


def test_unsupported_contract_version_status():
    decision = decision_from_exception(
        ValueError("unsupported contract version"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.
        SKIPPED_CONTRACT_VERSION_UNSUPPORTED
    )


def test_invalid_race_structure_status():
    decision = decision_from_exception(
        ValueError("invalid race structure: expected six boats"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_INVALID_RACE_STRUCTURE
    )


def test_prohibited_feature_status():
    decision = decision_from_exception(
        ValueError("prohibited feature: odds"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_PROHIBITED_FEATURE
    )


def test_output_integrity_error_status():
    decision = decision_from_exception(
        RuntimeError("output integrity error"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_OUTPUT_INTEGRITY_ERROR
    )


def test_unknown_exception_is_not_eligible():
    decision = decision_from_exception(
        RuntimeError("unexpected internal validation failure"),
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    assert decision.eligible is False
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_OUTPUT_INTEGRITY_ERROR
    )


def test_decision_serialization_is_deterministic():
    decision = skipped_decision(
        status=PreNightEligibilityStatus.SKIPPED_HASH_MISMATCH,
        reason="hash mismatch",
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
        details={"z": 1, "a": {"y": 2, "b": 3}},
    )
    first = decision.to_json()
    second = decision.to_json()
    assert first == second
    assert json.loads(first) == decision.to_dict()


def test_dry_run_manifest_contains_status():
    decision = skipped_decision(
        status=PreNightEligibilityStatus.SKIPPED_PROGRAM_UNAVAILABLE,
        reason="program unavailable",
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    fields = manifest_eligibility_fields(decision)
    assert fields["eligibility_status"] == (
        "SKIPPED_PROGRAM_UNAVAILABLE"
    )
    assert fields["eligible_for_pre_night"] is False
    assert fields["pit_eligibility"]["status"] == (
        "SKIPPED_PROGRAM_UNAVAILABLE"
    )


def test_live_execution_manifest_contains_status():
    decision = eligible_decision(
        race_date=RACE_DATE,
        as_of_time=AS_OF_TIME,
    )
    fields = manifest_eligibility_fields(decision)
    assert fields["eligibility_status"] == "ELIGIBLE"
    assert fields["eligible_for_pre_night"] is True
    assert fields["pit_eligibility"]["eligible"] is True


def test_existing_validation_exceptions_remain_enforced():
    error = ValueError("hash mismatch")
    status = classify_pre_night_exception(error)
    assert status is PreNightEligibilityStatus.SKIPPED_HASH_MISMATCH
    with pytest.raises(ValueError, match="hash mismatch"):
        raise error


def test_inconsistent_eligible_flag_is_rejected():
    with pytest.raises(ValueError, match="eligible"):
        PreNightEligibilityDecision(
            status=PreNightEligibilityStatus.ELIGIBLE,
            eligible=False,
            reason="invalid",
            race_date=RACE_DATE,
            as_of_time=AS_OF_TIME,
        )


def test_skipped_decision_rejects_eligible_status():
    with pytest.raises(ValueError, match="ELIGIBLE"):
        skipped_decision(
            status=PreNightEligibilityStatus.ELIGIBLE,
            reason="invalid",
            race_date=RACE_DATE,
            as_of_time=AS_OF_TIME,
        )
