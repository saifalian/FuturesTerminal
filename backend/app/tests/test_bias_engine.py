from app.services.signals.bias_engine import combine_scores


def test_combine_scores() -> None:
    out = combine_scores({"a": 1.0, "b": 1.0}, {"a": 0.5, "b": -0.4})
    assert out.long_score > 0
    assert out.short_score > 0
    assert 0 <= out.confidence <= 1
