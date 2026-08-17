"""Contract tests for separate pre-night storage roles.

These tests intentionally define the Option A CLI boundary before
the source implementation is changed.
"""

from boatrace_ai.cli.pre_night import build_parser


def _action_for(parser, option_string):
    """Return the argparse action for one option string."""
    action = parser._option_string_actions.get(option_string)
    assert action is not None, (
        f"missing required CLI option: {option_string}"
    )
    return action


def test_existing_data_root_is_explicitly_staging_root():
    parser = build_parser()
    action = _action_for(parser, "--data-root")

    assert action.dest == "data_root"
    assert "staging" in (action.help or "").lower()


def test_parser_declares_explicit_authority_root():
    parser = build_parser()
    action = _action_for(parser, "--authority-root")

    assert action.dest == "authority_root"
    assert "authority" in (action.help or "").lower()


def test_staging_and_authority_options_have_distinct_destinations():
    parser = build_parser()
    staging_action = _action_for(parser, "--data-root")
    authority_action = _action_for(parser, "--authority-root")

    assert staging_action is not authority_action
    assert staging_action.dest != authority_action.dest
    assert staging_action.dest == "data_root"
    assert authority_action.dest == "authority_root"
