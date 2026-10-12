from deadlock_detector.parsing import Event, node_sort_key, parse_line, to_edge


def test_tab_separated():
    assert parse_line("2025-10-31 12:15:59\tWAIT\tP109\tR008") == Event(
        "2025-10-31 12:15:59", "WAIT", "P109", "R008"
    )


def test_extra_whitespace_and_trailing_commas():
    assert parse_line("2025-10-31 12:15:59  HOLD P1,  R1,\n") == Event(
        "2025-10-31 12:15:59", "HOLD", "P1", "R1"
    )


def test_single_token_timestamp():
    assert parse_line("2025-10-31T12:15:59 wait P1 R0") == Event(
        "2025-10-31T12:15:59", "WAIT", "P1", "R0"
    )


def test_resource_first_order():
    assert parse_line("2025-10-25 00:29:15 HOLD R024 P105") == Event(
        "2025-10-25 00:29:15", "HOLD", "P105", "R024"
    )


def test_rejects_malformed_lines():
    for line in [
        "",
        "garbage",
        "2025-10-31 12:15:59 RELEASE P1 R1",
        "2025-10-31 12:15:59 WAIT P1 P2",
        "2025-10-31 12:15:59 WAIT X1 R1",
    ]:
        assert parse_line(line) is None, line


def test_edge_direction():
    assert to_edge(Event("t", "WAIT", "P1", "R1")) == (("P1", "R1"), "t")
    assert to_edge(Event("t", "HOLD", "P1", "R1")) == (("R1", "P1"), "t")


def test_node_sort_key_is_numeric():
    assert sorted(["P10", "P2", "P1"], key=node_sort_key) == ["P1", "P2", "P10"]
