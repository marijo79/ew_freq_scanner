from freqscan.parsing import parse_sweep_line, suppress_dc_spike, trim_edges


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


def test_suppress_dc_spike_default_width_replaces_three_center_bins():
    # n=7, center index = 3. width=1 replaces indices 2,3,4 via linear interpolation
    # between the flanking bins (index 1 and 5), discarding the spike at index 3.
    powers = [0.0, 1.0, 2.0, 999.0, 4.0, 5.0, 6.0]
    result = suppress_dc_spike(powers)
    assert result == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


def test_suppress_dc_spike_width_zero_replaces_only_center_bin():
    powers = [1.0, 2.0, 999.0, 4.0, 5.0]
    result = suppress_dc_spike(powers, width=0)
    assert result == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_suppress_dc_spike_does_not_mutate_input():
    powers = [1.0, 2.0, 999.0, 4.0, 5.0]
    suppress_dc_spike(powers, width=0)
    assert powers == [1.0, 2.0, 999.0, 4.0, 5.0]


def test_suppress_dc_spike_tiny_array_is_a_no_op_not_a_crash():
    assert suppress_dc_spike([], width=1) == []
    assert suppress_dc_spike([5.0], width=1) == [5.0]
    assert suppress_dc_spike([5.0, 6.0], width=1) == [5.0, 6.0]
