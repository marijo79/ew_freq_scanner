# ew_freq_scanner

EW Frequency Scanner project — live spectrum + waterfall plots for RTL-SDR or HackRF, selected via configuration.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# edit .env: set SDR=rtl or SDR=hackrf and adjust the matching section
```

## Running

```bash
python -m freqscan
```

Requires the underlying SDR CLI tool on PATH and hardware attached: `rtl_power` for `SDR=rtl`, `hackrf_sweep` for `SDR=hackrf`.

## Testing

```bash
pytest -q
```
