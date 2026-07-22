"""Regression tests for an ambiguous boat number/rate boundary.

The source token ``12100.00`` was previously parsed as boat number 121
and rate 00.00. For these verified source records, the correct split is
boat number 12 and rate 100.00, as confirmed by the corresponding
result records.
"""

import pytest

from boatrace_ai.parsers.program import FW_TRANS, PROGRAM_PATTERN


EXPECTED_RACER_ID = '4912'
EXPECTED_BOAT_EQUIPMENT = 12
EXPECTED_BOAT_RATE = '100.00'


CASES = [
    ('2023-05-15', 1424, '2 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00              9'),
    ('2023-05-15', 1524, '6 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00              1'),
    ('2023-05-16', 1315, '1 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 36            '),
    ('2023-05-17', 1269, '3 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364         10'),
    ('2023-05-17', 1378, '4 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364          1'),
    ('2023-05-18', 1475, '5 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364 14        '),
    ('2023-05-19', 1450, '4 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364 141      7'),
    ('2023-05-19', 1496, '2 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364 141      3'),
    ('2023-05-20', 1593, '3 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364 141 43   8'),
    ('2023-05-20', 1667, '5 4912中山将太27福井53B1 4.88 30.00 5.40 36.00 67  0.00 12100.00 364 141 43   2'),
]


@pytest.mark.parametrize(
    ("race_date", "source_line_number", "raw_line"),
    CASES,
)
def test_program_boat_number_before_100_percent_rate(
    race_date,
    source_line_number,
    raw_line,
):
    """A concatenated boat number 12 and rate 100.00 must stay separate."""
    del race_date, source_line_number

    normalized_line = raw_line.translate(FW_TRANS)

    # This exact boundary caused the former 121 / 00.00 split.
    assert "12100.00" in normalized_line

    match = PROGRAM_PATTERN.match(normalized_line)

    assert match is not None

    groups = match.groupdict()

    assert groups["racer_id"] == EXPECTED_RACER_ID
    assert int(groups["boat_no_equipment"]) == EXPECTED_BOAT_EQUIPMENT
    assert groups["boat_place2_rate_pct"] == EXPECTED_BOAT_RATE
