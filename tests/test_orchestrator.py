import asyncio
import io
import urllib.error
from datetime import datetime, timedelta, timezone
from email.message import Message

import pytest

from sports_skills import live_projection_orchestrator as orchestrator
from sports_skills.football import _connector as football_connector
from sports_skills.polymarket import _connector as polymarket_connector

NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)


def _market_response(home="Arsenal", away="Chelsea", updated_at=None, prices=(0.6, 0.4)):
    return {
        "status": True,
        "data": {
            "markets": [
                {
                    "updated_at": updated_at or NOW.isoformat(),
                    "outcomes": [
                        {"name": away, "price": prices[1]},
                        {"name": home, "price": prices[0]},
                    ],
                }
            ]
        },
    }


def _understat_response():
    return {"status": True, "data": {"teams": [{"xg": 1.2}], "shots": []}}


def _error(message, status_code=None, retry_after=None):
    result = {"status": False, "data": None, "message": message}
    if status_code is not None:
        result["status_code"] = status_code
    if retry_after is not None:
        result["retry_after"] = retry_after
    return result


def _assert_result_schema(result):
    assert set(result) >= {
        "status",
        "projection",
        "confidence",
        "sources_used",
        "sources_failed",
        "is_calibrated",
        "is_fallback",
        "warnings",
        "timestamp",
        "request_id",
    }
    assert result["status"] in {"ok", "degraded", "unavailable"}
    assert result["projection"] is None or isinstance(result["projection"], list)
    assert result["confidence"] is None or 0 <= result["confidence"] <= 1
    assert isinstance(result["sources_used"], list)
    assert isinstance(result["sources_failed"], list)
    assert isinstance(result["is_calibrated"], bool)
    assert isinstance(result["is_fallback"], bool)
    assert isinstance(result["warnings"], list)
    assert datetime.fromisoformat(result["timestamp"].replace("Z", "+00:00")).tzinfo is not None
    assert result["request_id"]


def _run(instance, **kwargs):
    return asyncio.run(
        instance.generate_projection_result("epl", "Arsenal", "Chelsea", understat_event_id="fixture-1", **kwargs)
    )


def test_both_connection_failures_return_documented_unavailable_result(monkeypatch):
    monkeypatch.setattr(
        orchestrator.football,
        "get_event_xg",
        lambda **kwargs: (_ for _ in ()).throw(ConnectionError("Understat offline")),
    )
    monkeypatch.setattr(
        orchestrator.polymarket,
        "search_markets",
        lambda **kwargs: (_ for _ in ()).throw(TimeoutError("Polymarket timed out")),
    )

    result = _run(orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW))

    _assert_result_schema(result)
    assert result["status"] == "unavailable"
    assert result["projection"] is None
    assert result["confidence"] is None
    assert result["sources_used"] == []
    assert {item["source"] for item in result["sources_failed"]} == {
        "understat",
        "polymarket",
        "historical",
    }
    assert result["is_fallback"] is False


@pytest.mark.parametrize("provider", ["understat", "polymarket"])
def test_each_provider_can_fail_without_blocking_the_other(monkeypatch, provider):
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _understat_response())
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _market_response())
    if provider == "understat":
        monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("HTTP 403", 403))
    else:
        monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _error("offline"))

    result = _run(orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW))

    _assert_result_schema(result)
    if provider == "understat":
        assert result["status"] == "degraded"
        assert result["projection"] == pytest.approx([0.6, 0.4])
        assert result["sources_used"] == ["polymarket"]
    else:
        assert result["status"] == "unavailable"
        assert result["projection"] is None
        assert result["sources_used"] == []


@pytest.mark.parametrize("status_code,retries", [(403, 0), (429, 2)])
def test_provider_http_statuses_are_preserved_and_retry_policy_is_bounded(monkeypatch, status_code, retries):
    calls = []
    delays = []
    failure_attempts = 1 if status_code == 403 else retries

    def provider(**kwargs):
        calls.append(1)
        if len(calls) <= failure_attempts:
            return _error(f"HTTP {status_code}", status_code, retry_after="1" if status_code == 429 else None)
        return _market_response()

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _understat_response())
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", provider)
    instance = orchestrator.SportsSkillsOrchestrator(
        clock=lambda: NOW,
        sleep=sleep,
        jitter=lambda delay: 0,
        retry_base_seconds=0.1,
        max_retries=2,
    )

    result = _run(instance)

    _assert_result_schema(result)
    if status_code == 403:
        assert len(calls) == 1
        assert result["projection"] is None
    else:
        assert len(calls) == 3
        assert delays == [1.0, 1.0]
        assert result["projection"] == pytest.approx([0.6, 0.4])


def test_malformed_empty_and_invalid_market_data_are_unavailable(monkeypatch):
    responses = [
        _error("malformed JSON", status_code=200),
        {"status": True, "data": {"markets": []}},
        {"status": True, "data": {"markets": [{"outcomes": None}]}},
        {"status": True, "data": {"markets": [{"outcomes": "unexpected"}]}},
        {"status": True, "data": None},
        _market_response(prices=(float("nan"), 0.4)),
        _market_response(prices=(True, False)),
        _market_response(prices=("0.6", "0.4")),
        _market_response(prices=(-0.1, 1.1)),
        _market_response(prices=(0.7, 0.7)),
    ]
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("no xG"))

    for response in responses:
        monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: response)
        result = _run(orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW))
        _assert_result_schema(result)
        assert result["status"] == "unavailable"
        assert result["projection"] is None


def test_stale_live_market_is_rejected(monkeypatch):
    stale = (NOW - timedelta(hours=1)).isoformat()
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("no xG"))
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _market_response(updated_at=stale))

    result = _run(orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW, max_market_age_seconds=900))

    assert result["status"] == "unavailable"
    assert result["projection"] is None
    assert any("stale" in warning.lower() for warning in result["warnings"])


def test_fresh_cache_is_used_after_live_sources_fail(monkeypatch):
    now = [NOW]
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("offline"))
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _market_response())
    instance = orchestrator.SportsSkillsOrchestrator(clock=lambda: now[0])
    first = _run(instance)
    assert first["status"] == "degraded"

    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _error("offline"))
    second = _run(instance)

    assert second["status"] == "degraded"
    assert second["projection"] == pytest.approx([0.6, 0.4])
    assert second["sources_used"] == ["local_cache"]
    assert second["is_fallback"] is True
    assert second["is_calibrated"] is False


def test_stale_cached_market_is_not_used(monkeypatch):
    now = [NOW]
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("offline"))
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _market_response())
    instance = orchestrator.SportsSkillsOrchestrator(clock=lambda: now[0], max_market_age_seconds=900)
    _run(instance)
    now[0] += timedelta(hours=1)
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _error("offline"))

    result = _run(instance)

    assert result["status"] == "unavailable"
    assert result["projection"] is None
    assert any("cache" in warning.lower() and "stale" in warning.lower() for warning in result["warnings"])
    assert any(item["source"] == "local_cache" for item in result["sources_failed"])


def test_historical_binary_baseline_is_laplace_smoothed_and_documented():
    historical = {
        "timestamp": (NOW - timedelta(days=3)).isoformat(),
        "source": "fixture",
        "outcomes": ["home", "home", "away"],
    }
    result = orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW).calculate_fallback_projection(
        "Arsenal", "Chelsea", "live unavailable", historical_data=historical
    )

    _assert_result_schema(result)
    assert result["status"] == "degraded"
    assert result["projection"] == pytest.approx([0.6, 0.4])
    assert result["confidence"] is None
    assert result["is_fallback"] is True
    assert result["is_calibrated"] is False
    assert any("Beta(1,1)" in warning for warning in result["warnings"])


def test_missing_or_malformed_history_returns_unavailable():
    instance = orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW)
    for historical in (
        None,
        {},
        {"timestamp": NOW.isoformat(), "outcomes": []},
        {"timestamp": "bad", "outcomes": ["home"]},
    ):
        result = instance.calculate_fallback_projection("Home", "Away", "sources failed", historical_data=historical)
        _assert_result_schema(result)
        assert result["status"] == "unavailable"
        assert result["projection"] is None


def test_future_dated_history_is_rejected():
    historical = {"timestamp": (NOW + timedelta(days=1)).isoformat(), "outcomes": ["home", "away"]}
    result = orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW).calculate_fallback_projection(
        "Home", "Away", "sources failed", historical_data=historical
    )

    assert result["status"] == "unavailable"
    assert result["projection"] is None
    assert any("future" in warning.lower() for warning in result["warnings"])


def test_fallback_exception_is_contained(monkeypatch):
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("offline"))
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _error("offline"))
    instance = orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW)
    monkeypatch.setattr(
        instance,
        "calculate_fallback_projection",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("fallback bug")),
    )

    result = _run(instance)

    _assert_result_schema(result)
    assert result["status"] == "unavailable"
    assert result["projection"] is None
    assert any("fallback bug" in warning for warning in result["warnings"])


def test_provider_timeout_is_contained(monkeypatch):
    async def slow_understat(event_id):
        await asyncio.sleep(0.05)
        return _understat_response()

    async def no_market(sport, team_home, team_away):
        return _error("offline")

    instance = orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW, request_timeout_seconds=0.001)
    monkeypatch.setattr(instance, "fetch_understat_xg", slow_understat)
    monkeypatch.setattr(instance, "fetch_polymarket_markets", no_market)

    result = _run(instance)

    _assert_result_schema(result)
    assert result["status"] == "unavailable"
    assert any("timed out" in item["reason"].lower() for item in result["sources_failed"])


def test_legacy_adapter_keeps_boolean_envelope(monkeypatch):
    monkeypatch.setattr(orchestrator.football, "get_event_xg", lambda **kwargs: _error("offline"))
    monkeypatch.setattr(orchestrator.polymarket, "search_markets", lambda **kwargs: _market_response())

    result = asyncio.run(
        orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW).generate_unified_projection(
            "epl", "Arsenal", "Chelsea", understat_event_id="fixture-1"
        )
    )

    assert isinstance(result["status"], bool)
    assert result["status"] is True
    assert result["data"]["projection"] == pytest.approx([0.6, 0.4])
    assert result["data"]["output_vector"] == pytest.approx([0.6, 0.4])


def test_non_string_input_returns_structured_unavailable_result(monkeypatch):
    def unexpected_call(**kwargs):
        raise AssertionError("provider should not be called")

    monkeypatch.setattr(orchestrator.polymarket, "search_markets", unexpected_call)
    result = asyncio.run(
        orchestrator.SportsSkillsOrchestrator(clock=lambda: NOW).generate_projection_result("epl", None, "Away")
    )

    _assert_result_schema(result)
    assert result["status"] == "unavailable"
    assert result["projection"] is None


def _http_error(url, status, retry_after=None):
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return urllib.error.HTTPError(url, status, "error", headers, io.BytesIO(b"failure"))


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return b'{"ok": true}'


def test_polymarket_403_is_not_retried_and_request_has_timeout(monkeypatch):
    calls = []

    def urlopen(request, timeout):
        calls.append(timeout)
        raise _http_error(request.full_url, 403)

    monkeypatch.setattr(polymarket_connector.urllib.request, "urlopen", urlopen)

    raw, error = polymarket_connector._live_http_fetch("https://example.test", None, max_retries=3)

    assert raw is None
    assert error["status_code"] == 403
    assert calls == [30]


def test_polymarket_429_retry_after_is_respected_deterministically(monkeypatch):
    calls = []
    delays = []

    def urlopen(request, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            raise _http_error(request.full_url, 429, retry_after="2")
        return _Response()

    monkeypatch.setattr(polymarket_connector.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(polymarket_connector.time, "sleep", delays.append)
    monkeypatch.setattr(polymarket_connector.random, "uniform", lambda low, high: 1.0)

    raw, error = polymarket_connector._live_http_fetch("https://example.test", None, max_retries=2)

    assert error is None
    assert raw == b'{"ok": true}'
    assert calls == [30, 30]
    assert delays == [2.0]


def test_understat_429_retry_after_is_respected_deterministically(monkeypatch):
    calls = []
    delays = []

    def urlopen(request, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            raise _http_error(request.full_url, 429, retry_after="3")
        return _Response()

    monkeypatch.setattr(football_connector.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(football_connector.time, "sleep", delays.append)
    monkeypatch.setattr(football_connector.random, "uniform", lambda low, high: 1.0)

    raw, error = football_connector._live_http_fetch("https://example.test", rate_limiter=None, max_retries=1)

    assert error is None
    assert raw == b'{"ok": true}'
    assert calls == [30, 30]
    assert delays == [3.0]


def test_understat_403_is_not_retried(monkeypatch):
    calls = []

    def urlopen(request, timeout):
        calls.append(timeout)
        raise _http_error(request.full_url, 403)

    monkeypatch.setattr(football_connector.urllib.request, "urlopen", urlopen)

    raw, error = football_connector._live_http_fetch("https://example.test", rate_limiter=None, max_retries=3)

    assert raw is None
    assert error["status_code"] == 403
    assert calls == [30]


def test_polymarket_malformed_json_is_marked_without_retry(monkeypatch):
    calls = []
    monkeypatch.setattr(
        polymarket_connector,
        "_http_fetch",
        lambda url, rate_limiter: calls.append(url) or (b"{", None),
    )

    result = polymarket_connector._get_json("https://example.test", None, "fixture", 0)

    assert result["malformed_json"] is True
    assert len(calls) == 1
