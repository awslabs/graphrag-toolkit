# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Guards against the benchmark harness choosing its own inference mode.

WikihowBenchmarkExtract used to omit use_batch and inherit a default of True,
so runs measured Bedrock batch queue time rather than the pipeline. Two runs on
byte-identical config finished 22.5h and 3.96h apart. Nothing errored, so only
the timings gave it away.

Two halves. TestEnvBool and TestEnvInt are ordinary unit tests of the readers.
Everything below them checks the shape of benchmark_extract.py by parsing it
with ast, because the module imports graphrag_toolkit and llama_index and
benchmarks/tests runs without either installed.
"""

import ast
import pathlib

import pytest

from benchmarks.utils.benchmark_env import env_bool, env_int

SCRIPT = (
    pathlib.Path(__file__).resolve().parents[1] / 'scripts' / 'benchmark_extract.py'
)


# ---------------------------------------------------------------- helpers


def _tree():
    return ast.parse(SCRIPT.read_text())


def _function(name, scope=None):
    for node in ast.walk(scope or _tree()):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f'{name} not found in {SCRIPT.name}')


def _class(name):
    for node in ast.walk(_tree()):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f'class {name} not found in {SCRIPT.name}')


def _defaults_by_param(fn):
    """Map each positional parameter to its default node, or None if it has none."""
    args = fn.args.args
    defaults = fn.args.defaults
    offset = len(args) - len(defaults)
    return {
        a.arg: (defaults[i - offset] if i >= offset else None)
        for i, a in enumerate(args)
    }


def _call_kwargs(class_name, callee='run_benchmark_extract'):
    run_test = _function('_run_test', scope=_class(class_name))
    for node in ast.walk(run_test):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == callee:
            return [kw.arg for kw in node.keywords]
    raise AssertionError(f'{class_name}._run_test does not call {callee}')


# ------------------------------------------------- unit tests: env readers


class TestEnvBool:

    def test_returns_default_when_unset(self, monkeypatch):
        monkeypatch.delenv('X_FLAG', raising=False)
        assert env_bool('X_FLAG', True) is True
        assert env_bool('X_FLAG', False) is False

    @pytest.mark.parametrize('raw', ['true', 'True', 'TRUE', ' true '])
    def test_truthy_spellings(self, monkeypatch, raw):
        monkeypatch.setenv('X_FLAG', raw)
        assert env_bool('X_FLAG', False) is True

    @pytest.mark.parametrize('raw', ['false', 'False', 'FALSE', ' false '])
    def test_falsey_spellings(self, monkeypatch, raw):
        monkeypatch.setenv('X_FLAG', raw)
        assert env_bool('X_FLAG', True) is False

    def test_empty_string_falls_back_to_default(self, monkeypatch):
        # The harness exports unset variables as empty strings.
        monkeypatch.setenv('X_FLAG', '')
        assert env_bool('X_FLAG', True) is True

    @pytest.mark.parametrize('raw', ['flase', 'yes', '1', 'on', 'no', '0', 'off'])
    def test_unrecognised_value_raises(self, monkeypatch, raw):
        monkeypatch.setenv('X_FLAG', raw)
        with pytest.raises(ValueError, match='X_FLAG'):
            env_bool('X_FLAG', True)

    def test_omitting_default_makes_the_variable_required(self, monkeypatch):
        monkeypatch.delenv('X_FLAG', raising=False)
        with pytest.raises(ValueError, match='X_FLAG'):
            env_bool('X_FLAG')

    def test_empty_string_is_required_too(self, monkeypatch):
        monkeypatch.setenv('X_FLAG', '')
        with pytest.raises(ValueError, match='X_FLAG'):
            env_bool('X_FLAG')


class TestEnvInt:

    def test_returns_default_when_unset(self, monkeypatch):
        monkeypatch.delenv('X_NUM', raising=False)
        assert env_int('X_NUM', 2) == 2

    def test_empty_string_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv('X_NUM', '')
        assert env_int('X_NUM', 15000) == 15000

    def test_parses_value(self, monkeypatch):
        monkeypatch.setenv('X_NUM', '64')
        assert env_int('X_NUM', 2) == 64

    def test_omitting_default_makes_the_variable_required(self, monkeypatch):
        monkeypatch.delenv('X_NUM', raising=False)
        with pytest.raises(ValueError, match='X_NUM'):
            env_int('X_NUM')

    def test_non_numeric_raises(self, monkeypatch):
        monkeypatch.setenv('X_NUM', 'many')
        with pytest.raises(ValueError, match='X_NUM'):
            env_int('X_NUM', 2)


# --------------------------------------- structural: benchmark_extract.py


class TestInferenceModeIsExplicit:
    """
    Dropping the signature default is not enough on its own. A call site
    reading env_bool('BENCHMARK_USE_BATCH', True) re-arms the same trap one
    layer down, since anyone unaware of the variable still gets batch.
    """


    def test_run_benchmark_extract_has_no_use_batch_default(self):
        fn = _function('run_benchmark_extract')
        defaults = _defaults_by_param(fn)
        assert 'use_batch' in defaults, 'run_benchmark_extract lost its use_batch parameter'
        assert defaults['use_batch'] is None, (
            'use_batch must have no default so every caller states its inference '
            'mode explicitly'
        )

    @pytest.mark.parametrize(
        'class_name', ['WikihowBenchmarkExtract', 'PgaBenchmarkExtract', 'ConcurrentQaBenchmarkExtract']
    )
    def test_every_benchmark_passes_use_batch(self, class_name):
        assert 'use_batch' in _call_kwargs(class_name), (
            f'{class_name} must pass use_batch explicitly'
        )


    @pytest.mark.parametrize(
        'class_name', ['WikihowBenchmarkExtract', 'PgaBenchmarkExtract']
    )
    def test_call_site_does_not_default_the_env_read(self, class_name):
        run_test = _function('_run_test', scope=_class(class_name))
        for node in ast.walk(run_test):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, 'id', None) != 'env_bool':
                continue
            # Any second argument re-arms the trap, literal or not: a
            # module constant is an ast.Name and used to slip through.
            assert not node.args[1:] and not node.keywords, (
                f'{class_name} gives BENCHMARK_USE_BATCH a default, which restores the implicit inference mode'
            )

    def test_concurrentqa_derives_its_mode_and_does_not_read_the_variable(self):
        # Keys off BENCHMARK_IS_PROTOTYPE instead. Asserted so the exemption
        # is deliberate rather than a test that passes vacuously.
        run_test = _function('_run_test', scope=_class('ConcurrentQaBenchmarkExtract'))
        calls = [n for n in ast.walk(run_test)
                 if isinstance(n, ast.Call) and getattr(n.func, 'id', None) == 'env_bool']
        assert not calls


class TestExtractionConfigIsOverridable:

    def _assigned_value(self, attribute):
        for node in ast.walk(_tree()):
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if isinstance(target, ast.Attribute) and target.attr == attribute:
                    return node.value
        raise AssertionError(f'no assignment to {attribute} in {SCRIPT.name}')

    @pytest.mark.parametrize(
        'attribute', ['extraction_batch_size', 'extraction_num_workers']
    )
    def test_not_a_hardcoded_constant(self, attribute):
        value = self._assigned_value(attribute)
        assert not isinstance(value, ast.Constant), (
            f'GraphRAGConfig.{attribute} is hardcoded, so the environment cannot '
            f'override it and a sweep silently reports the fixed value'
        )


class TestVariableNameIsPinned:
    """
    The Python call sites and the harness export are joined only by the string
    'BENCHMARK_USE_BATCH'. A typo in either file leaves the benchmark requiring
    a variable nothing sets, and every test above still passed when I renamed
    the call to BENCHMARK_USE_BACTH.
    """

    BUILD_TESTS = (
        pathlib.Path(__file__).resolve().parents[2]
        / 'integration-tests' / 'build-tests.sh'
    )

    def test_call_sites_read_the_expected_name(self):
        names = set()
        for node in ast.walk(_tree()):
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'env_bool':
                names.add(node.args[0].value)
        assert names == {'BENCHMARK_USE_BATCH'}, (
            f'unexpected env_bool variable name(s): {sorted(names)}'
        )

    def test_harness_exports_the_same_name(self):
        assert 'BENCHMARK_USE_BATCH' in self.BUILD_TESTS.read_text(), (
            'build-tests.sh does not export BENCHMARK_USE_BATCH, so the '
            'benchmarks would require a variable that never reaches the notebook'
        )

    @pytest.mark.parametrize('name', ['EXTRACTION_NUM_WORKERS', 'EXTRACTION_BATCH_SIZE'])
    def test_harness_exports_the_extraction_variables(self, name):
        assert name in self.BUILD_TESTS.read_text()
