import logging

from job_plat.config.logconfig import setup_logging


def test_console_logging_uses_json_formatter(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "job_plat.config.logconfig.logging.config.dictConfig",
        lambda config: captured.update(config),
    )

    setup_logging(logging.INFO)

    assert captured["handlers"]["console"]["formatter"] == "json"
