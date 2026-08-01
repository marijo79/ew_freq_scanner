from freqscan.parsing import parse_sweep_line, trim_edges


def _line(powers):
    parts = ["2024-01-01", "12:00:00", "100000000.0", "100100000.0", "1000.0", str(len(powers))]
    parts += [str(p) for p in powers]
    return ", ".join(parts)


def test_parse_valid_line():
    raw = _line([-50.0, -49.0, -48.0, -47.0, -46.0])
    result = parse_sweep_line(raw)
    assert result is not None
    assert result.hz_low == 100000000.0
    assert result.hz_step == 1000.0
    assert result.powers == [-50.0, -49.0, -48.0, -47.0, -46.0]


def test_parse_blank_line():
    assert parse_sweep_line("") is None
    assert parse_sweep_line("   ") is None


def test_parse_too_few_columns():
    assert parse_sweep_line("2024-01-01, 12:00:00, 100000000.0") is None


def test_parse_non_numeric_field():
    raw = "2024-01-01, 12:00:00, not_a_number, 100100000.0, 1000.0, 1, -50.0"
    assert parse_sweep_line(raw) is None


def test_trim_edges_basic():
    powers = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    freqs, trimmed = trim_edges(hz_low=0.0, hz_step=1.0, powers=powers, edge_trim=0.2)
    assert trimmed == [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    assert freqs == [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]


def test_trim_edges_minimum_trim_boundary():
    # n=4, int(4 * 0.05) == 0, but max(1, ...) forces a trim of at least one bin per edge
    powers = [1.0, 2.0, 3.0, 4.0]
    freqs, trimmed = trim_edges(hz_low=10.0, hz_step=2.0, powers=powers, edge_trim=0.05)
    assert trimmed == [2.0, 3.0]
    assert freqs == [12.0, 14.0]
