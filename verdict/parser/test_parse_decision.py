from parse_decision import parse_decision

def test_grid_drop_penalty():
    result = parse_decision("verdict/data/raw/test.pdf")
    assert result["own_car"] == 77
    assert result["other_car"] == 20
    assert result["penalty_points"] == 3
    assert result["turn"] == 6


def test_offence_wording_normalized():
    result = parse_decision("verdict/data/raw/offence.pdf")
    assert result["own_car"] == 77
    assert result["other_car"] == 4
    assert result["penalty_points"] == 2
    assert result["turn"] == 1
    assert "Infringement" in result
    assert "Offence" not in result


def test_warning_no_penalty_points():
    result = parse_decision("verdict/data/raw/test-no-collision.pdf")
    assert result["own_car"] == 5
    assert result["other_car"] == 43
    assert result["penalty_points"] is None
    assert result["turn"] == 5


def test_no_further_action():
    result = parse_decision("verdict/data/raw/test-no-decision.pdf")
    assert result["own_car"] == 44
    assert result["other_car"] == 10
    assert result["penalty_points"] is None
    assert result["turn"] == 6
