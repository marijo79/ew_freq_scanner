from freqscan.config import Settings
from freqscan.plotting import run
from freqscan.sdr import build_backend


def main() -> None:
    settings = Settings()
    run(build_backend(settings), settings.waterfall_rows)


if __name__ == "__main__":
    main()
