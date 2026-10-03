from app.services.execution.risk_checks import validate_order


def test_validate_order() -> None:
    ok, _ = validate_order(0.1, 500, 100)
    assert ok

    ok2, _ = validate_order(-1, 500, 100)
    assert not ok2
