import pytest

from retail_lakehouse.quality import VALID_LAYERS, build_checks, evaluate


@pytest.mark.parametrize(
    "actual, op, expected, result",
    [
        (0, "==", 0, True),
        (1, "==", 0, False),
        (112_653, ">=", 100_000, True),
        (50, ">=", 100_000, False),
        (5, "<=", 10, True),
    ],
)
def test_evaluate(actual, op, expected, result):
    assert evaluate(actual, op, expected) is result


def test_missing_value_always_fails():
    assert evaluate(None, "==", 0) is False


def test_unknown_operator_raises():
    with pytest.raises(ValueError):
        evaluate(1, "!=", 0)


def test_build_checks_has_unique_names():
    names = [c[0] for c in build_checks("retail_lakehouse", 100_000)]
    assert len(names) == len(set(names)) == 10


def test_build_checks_uses_the_given_catalog_everywhere():
    for _, _, sql, _, _ in build_checks("my_catalog", 100_000):
        assert "my_catalog." in sql


def test_min_rows_threshold_is_applied():
    checks = {c[0]: c for c in build_checks("retail_lakehouse", 12_345)}
    assert checks["gold_fact_min_rows"][4] == 12_345


def test_every_check_has_a_valid_layer_and_operator():
    for name, layer, _, op, _ in build_checks("retail_lakehouse", 1):
        assert layer in VALID_LAYERS, name
        assert op in {"==", ">=", "<=", ">", "<"}, name