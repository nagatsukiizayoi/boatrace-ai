from __future__ import annotations

import argparse
import ast
import inspect
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from boatrace_ai.cli import pre_night as pre_night_cli
from boatrace_ai.pipelines import prospective_v3


def _option_action(
    parser: argparse.ArgumentParser,
    option: str,
) -> argparse.Action:
    matches = [
        action
        for action in parser._actions
        if option in action.option_strings
    ]

    assert len(matches) == 1, (
        f'expected one action for {option!r}, '
        f'found {len(matches)}'
    )

    return matches[0]


def _collect_paths(value: Any) -> list[Path]:
    if isinstance(value, Path):
        return [value]

    if isinstance(value, Mapping):
        collected: list[Path] = []
        for item in value.values():
            collected.extend(_collect_paths(item))
        return collected

    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        collected = []
        for item in value:
            collected.extend(_collect_paths(item))
        return collected

    if hasattr(value, '__dict__'):
        return _collect_paths(vars(value))

    return []


def _is_args_authority_root(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == 'authority_root'
        and isinstance(node.value, ast.Name)
        and node.value.id == 'args'
    )


def test_authority_root_is_required() -> None:
    parser = pre_night_cli.build_parser()
    action = _option_action(parser, '--authority-root')

    assert action.dest == 'authority_root'
    assert action.required is True


def test_prospective_paths_accept_explicit_authority_root() -> None:
    signature = inspect.signature(prospective_v3._paths)

    assert 'authority_root' in signature.parameters, (
        '_paths() must accept an explicit authority_root '
        'instead of deriving authority storage from data_root'
    )


def test_prospective_paths_use_authority_root_directly(
    tmp_path: Path,
) -> None:
    authority_root = (
        tmp_path / 'prospective' / 'pre_night'
    )

    paths = prospective_v3._paths(
        authority_root=authority_root,
        race_date='2026-08-18',
        run_id='option-a-path-contract',
    )

    discovered_paths = _collect_paths(paths)

    assert discovered_paths, (
        '_paths() did not expose any pathlib.Path values'
    )

    resolved_authority_root = authority_root.resolve()

    for discovered in discovered_paths:
        resolved = discovered.resolve()

        assert resolved.is_relative_to(
            resolved_authority_root
        ), (
            f'{resolved} escapes authority root '
            f'{resolved_authority_root}'
        )

        normalized = resolved.as_posix()
        assert (
            'prospective/pre_night/prospective/pre_night'
            not in normalized
        ), normalized


def test_main_forwards_authority_root_explicitly() -> None:
    source = inspect.getsource(pre_night_cli.main)
    tree = ast.parse(source)

    forwarding_keywords = [
        keyword
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if (
            keyword.arg == 'authority_root'
            and _is_args_authority_root(keyword.value)
        )
    ]

    assert forwarding_keywords, (
        'main() must forward args.authority_root using '
        'an explicit authority_root keyword argument'
    )


def test_main_no_longer_rejects_authority_root_as_unwired() -> None:
    source = inspect.getsource(pre_night_cli.main)
    tree = ast.parse(source)

    string_constants = [
        node.value
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        )
    ]

    assert (
        '--authority-root runtime wiring is not implemented'
        not in string_constants
    )
