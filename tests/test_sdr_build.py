import json

from freqscan.config import DeviceConfig, HackRFSettings, KafkaSettings, RangeConfig, RTLSettings, Settings
from freqscan.sdr import build_backend
from freqscan.sdr.base import CompositeBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend

_RTL = RTLSettings(devices=[DeviceConfig(id=0, freq_start="80M", freq_stop="120M")])
_HACKRF = HackRFSettings(bin_width=20000, ranges=[RangeConfig(freq_start=850, freq_stop=950)])


class _FakeProducer:
    """Enough of confluent_kafka.Producer's surface for build_backend()'s metadata
    publish + flush at startup — no real broker, no offset tracking needed here."""

    def __init__(self):
        self.produced: list[tuple] = []

    def produce(self, topic, key, value, partition=-1, on_delivery=None) -> None:
        self.produced.append((topic, key, value, partition))

    def poll(self, timeout: float) -> None:
        pass

    def flush(self, timeout: float = 5.0) -> int:
        return 0


def test_build_backend_rtl_only():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=None)
    backend = build_backend(settings)
    assert isinstance(backend, RTLBackend)
    assert len(backend.channels) == 1


def test_build_backend_hackrf_only():
    settings = Settings(_env_file=None, rtl=None, hackrf=_HACKRF)
    backend = build_backend(settings)
    assert isinstance(backend, HackRFBackend)
    assert len(backend.channels) == 1


def test_build_backend_both_combines_channels_rtl_first():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=_HACKRF)
    backend = build_backend(settings)
    assert isinstance(backend, CompositeBackend)
    assert len(backend.channels) == 2
    assert backend.channels[0].label == "RTL: Dev0 80-120 MHz"
    assert backend.channels[1].label == "HackRF: 850-950 MHz"


def test_build_backend_without_kafka_has_no_publisher_or_detectors():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=None, kafka=None)
    backend = build_backend(settings)
    assert backend._publisher is None
    assert backend._detectors is None
    assert backend._partitions is None
    assert backend.publisher is None


def test_build_backend_kafka_disabled_has_no_publisher_or_detectors():
    kafka = KafkaSettings(enabled=False, bootstrap_servers="broker:9098")
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=None, kafka=kafka)
    backend = build_backend(settings)
    assert backend._publisher is None
    assert backend._detectors is None
    assert backend._partitions is None


def test_build_backend_kafka_enabled_builds_per_device_margins(monkeypatch):
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: _FakeProducer())

    kafka = KafkaSettings(
        enabled=True,
        bootstrap_servers="broker:9098",
        signal_margin_db=8.0,
        rtl_margins_db={0: 5.0},
    )
    settings = Settings(
        _env_file=None,
        rtl=RTLSettings(
            devices=[
                DeviceConfig(id=0, freq_start="80M", freq_stop="120M"),
                DeviceConfig(id=1, freq_start="420M", freq_stop="470M"),
            ]
        ),
        hackrf=None,
        kafka=kafka,
    )
    backend = build_backend(settings)
    assert backend._publisher is not None
    assert backend.publisher is backend._publisher
    assert [d.margin_db for d in backend._detectors] == [5.0, 8.0]
    assert backend._partitions == [0, 1]


def test_build_backend_kafka_enabled_builds_per_device_spatial_margins(monkeypatch):
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: _FakeProducer())

    kafka = KafkaSettings(
        enabled=True,
        bootstrap_servers="broker:9098",
        spatial_margin_db=8.0,
        rtl_spatial_margins_db={0: 5.0},
    )
    settings = Settings(
        _env_file=None,
        rtl=RTLSettings(
            devices=[
                DeviceConfig(id=0, freq_start="80M", freq_stop="120M"),
                DeviceConfig(id=1, freq_start="420M", freq_stop="470M"),
            ]
        ),
        hackrf=None,
        kafka=kafka,
    )
    backend = build_backend(settings)
    assert [d.spatial_margin_db for d in backend._detectors] == [5.0, 8.0]


def test_build_backend_kafka_enabled_builds_per_device_prominence_margins(monkeypatch):
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: _FakeProducer())

    kafka = KafkaSettings(
        enabled=True,
        bootstrap_servers="broker:9098",
        prominence_margin_db=20.0,
        rtl_prominence_margins_db={0: 15.0},
    )
    settings = Settings(
        _env_file=None,
        rtl=RTLSettings(
            devices=[
                DeviceConfig(id=0, freq_start="80M", freq_stop="120M"),
                DeviceConfig(id=1, freq_start="420M", freq_stop="470M"),
            ]
        ),
        hackrf=None,
        kafka=kafka,
    )
    backend = build_backend(settings)
    assert [d.prominence_margin_db for d in backend._detectors] == [15.0, 20.0]


def test_build_backend_composite_exposes_shared_publisher(monkeypatch):
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: _FakeProducer())

    kafka = KafkaSettings(enabled=True, bootstrap_servers="broker:9098")
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=_HACKRF, kafka=kafka)
    backend = build_backend(settings)
    assert isinstance(backend, CompositeBackend)
    assert backend.publisher is not None
    assert all(b._publisher is backend.publisher for b in backend.backends)


def test_build_backend_composite_assigns_sequential_partitions_rtl_first(monkeypatch):
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: _FakeProducer())

    kafka = KafkaSettings(enabled=True, bootstrap_servers="broker:9098")
    rtl = RTLSettings(
        devices=[
            DeviceConfig(id=0, freq_start="80M", freq_stop="120M"),
            DeviceConfig(id=1, freq_start="420M", freq_stop="470M"),
        ]
    )
    hackrf = HackRFSettings(
        bin_width=20000,
        ranges=[
            RangeConfig(freq_start=850, freq_stop=950),
            RangeConfig(freq_start=2300, freq_stop=2500),
        ],
    )
    settings = Settings(_env_file=None, rtl=rtl, hackrf=hackrf, kafka=kafka)
    backend = build_backend(settings)
    rtl_backend, hackrf_backend = backend.backends
    assert rtl_backend._partitions == [0, 1]
    assert hackrf_backend._partitions == [2, 3]


def _metadata_messages(producer: _FakeProducer, metadata_topic: str) -> list[dict]:
    return [json.loads(value) for topic, _key, value, _partition in producer.produced if topic == metadata_topic]


def test_build_backend_publishes_rtl_metadata_once_per_channel(monkeypatch):
    producer = _FakeProducer()
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: producer)

    kafka = KafkaSettings(enabled=True, bootstrap_servers="broker:9098")
    rtl = RTLSettings(
        devices=[
            DeviceConfig(id=0, freq_start="80M", freq_stop="120M", bin_width=10_000),
            DeviceConfig(id=1, freq_start="420M", freq_stop="470M", bin_width=10_000),
        ]
    )
    settings = Settings(_env_file=None, rtl=rtl, hackrf=None, kafka=kafka)
    build_backend(settings)

    messages = _metadata_messages(producer, kafka.metadata_topic)
    assert messages == [
        {
            "channel": "RTL: Dev0 80-120 MHz",
            "freq_start_hz": 80e6,
            "freq_stop_hz": 120e6,
            "bin_width_hz": 10_000,
            "n_bins": 4000,
        },
        {
            "channel": "RTL: Dev1 420-470 MHz",
            "freq_start_hz": 420e6,
            "freq_stop_hz": 470e6,
            "bin_width_hz": 10_000,
            "n_bins": 5000,
        },
    ]
    # metadata for each channel goes to the same partition as that channel's signals
    partitions = [p for topic, _k, _v, p in producer.produced if topic == kafka.metadata_topic]
    assert partitions == [0, 1]


def test_build_backend_publishes_hackrf_metadata_using_shared_bin_width(monkeypatch):
    producer = _FakeProducer()
    monkeypatch.setattr("freqscan.sdr.build_producer", lambda kafka: producer)

    kafka = KafkaSettings(enabled=True, bootstrap_servers="broker:9098")
    hackrf = HackRFSettings(
        bin_width=20_000,
        ranges=[
            RangeConfig(freq_start=850, freq_stop=950),
            RangeConfig(freq_start=2300, freq_stop=2500),
        ],
    )
    settings = Settings(_env_file=None, rtl=None, hackrf=hackrf, kafka=kafka)
    build_backend(settings)

    messages = _metadata_messages(producer, kafka.metadata_topic)
    # bin_width_hz is HackRFSettings.bin_width for every range — hackrf_sweep only takes
    # one -w value for its entire process, shared across all configured ranges.
    assert messages == [
        {
            "channel": "HackRF: 850-950 MHz",
            "freq_start_hz": 850e6,
            "freq_stop_hz": 950e6,
            "bin_width_hz": 20_000,
            "n_bins": 5000,
        },
        {
            "channel": "HackRF: 2300-2500 MHz",
            "freq_start_hz": 2300e6,
            "freq_stop_hz": 2500e6,
            "bin_width_hz": 20_000,
            "n_bins": 10_000,
        },
    ]


def test_build_backend_without_kafka_does_not_call_flush_or_publish_metadata():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=None, kafka=None)
    # No monkeypatch of build_producer: if streaming were mistakenly attempted, this
    # would try to construct a real confluent_kafka.Producer and fail/hang.
    backend = build_backend(settings)
    assert backend.publisher is None
