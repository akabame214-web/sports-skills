"""Async read-only orchestration for Understat xG and Polymarket markets."""

from __future__ import annotations

import asyncio
import email.utils
import math
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sports_skills import football, polymarket


def _team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _outcome_price(market: dict, team: str) -> float | None:
    team_key = _team_key(team)
    outcomes = market.get("outcomes")
    if not isinstance(outcomes, (list, tuple)):
        return None
    for outcome in outcomes:
        if not isinstance(outcome, dict) or _team_key(str(outcome.get("name", ""))) != team_key:
            continue
        raw_price = outcome.get("price")
        if isinstance(raw_price, bool) or not isinstance(raw_price, (int, float)):
            return None
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            return None
        return price if math.isfinite(price) and 0 <= price <= 1 else None
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _validated_probability_vector(values: Any) -> tuple[list[float] | None, bool]:
    if not isinstance(values, (list, tuple)) or len(values) != 2:
        return None, False
    vector = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, False
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0 or numeric > 1:
            return None, False
        vector.append(numeric)
    total = sum(vector)
    if total <= 0 or abs(total - 1.0) > 0.02:
        return None, False
    normalized = abs(total - 1.0) > 1e-9
    if normalized:
        vector = [value / total for value in vector]
    return vector, normalized


def _status_code(response: Any) -> int | None:
    if not isinstance(response, dict):
        return None
    code = response.get("status_code")
    if isinstance(code, int):
        return code
    message = str(response.get("message", ""))
    match = re.search(r"HTTP(?: error)?\s*\(?([1-5][0-9]{2})", message, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _retry_after_seconds(response: Any, now: datetime) -> float | None:
    if not isinstance(response, dict):
        return None
    retry_after = response.get("retry_after")
    headers = response.get("headers")
    if retry_after is None and isinstance(headers, dict):
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after is None:
        return None
    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        try:
            retry_at = email.utils.parsedate_to_datetime(str(retry_after))
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at.astimezone(timezone.utc) - now).total_seconds())


def _failure(source: str, response: Any, default: str) -> dict:
    if isinstance(response, dict):
        reason = response.get("message") or default
        code = _status_code(response)
        item = {"source": source, "reason": str(reason)}
        if code is not None:
            item["status_code"] = code
        return item
    return {"source": source, "reason": str(response) if response is not None else default}


def _valid_understat_result(response: Any) -> tuple[bool, str | None]:
    if not isinstance(response, dict) or response.get("status") is not True:
        return False, str(response.get("message", "Understat request failed")) if isinstance(
            response, dict
        ) else "Invalid Understat response"
    data = response.get("data")
    teams = data.get("teams") if isinstance(data, dict) else None
    if not isinstance(teams, list) or len(teams) != 2:
        return False, "Understat response is missing the two team xG records"
    for team in teams:
        if not isinstance(team, dict):
            return False, "Understat response contains a malformed team record"
        xg = team.get("xg")
        if isinstance(xg, bool) or not isinstance(xg, (int, float)) or not math.isfinite(float(xg)) or xg < 0:
            return False, "Understat response contains invalid xG"
    return True, None


def _historical_vector(data: Any, now: datetime) -> tuple[list[float] | None, str | None, str | None]:
    if not isinstance(data, dict):
        return None, None, "No historical dataset was supplied"
    timestamp = _parse_timestamp(data.get("timestamp"))
    outcomes = data.get("outcomes")
    if timestamp is None:
        return None, None, "Historical dataset has no valid timestamp"
    if timestamp > now + timedelta(minutes=2):
        return None, None, "Historical dataset timestamp is in the future"
    if not isinstance(outcomes, list) or not outcomes:
        return None, None, "Historical dataset has no binary outcomes"
    normalized = [str(outcome).strip().lower() for outcome in outcomes]
    if any(outcome not in {"home", "away", "draw"} for outcome in normalized):
        return None, None, "Historical dataset contains an unsupported outcome"
    home_wins = normalized.count("home") + 0.5 * normalized.count("draw")
    total = len(normalized)
    home_probability = (home_wins + 1.0) / (total + 2.0)
    return [home_probability, 1.0 - home_probability], _utc_iso(timestamp), None


class SportsSkillsOrchestrator:
    """Combine read-only sources while returning an explicit result contract."""

    def __init__(
        self,
        *,
        request_timeout_seconds: float = 90.0,
        max_retries: int = 0,
        retry_base_seconds: float = 0.5,
        max_retry_after_seconds: float = 8.0,
        max_market_age_seconds: float = 900.0,
        sleep=None,
        jitter=None,
        clock=None,
    ):
        self.request_timeout_seconds = max(0.01, float(request_timeout_seconds))
        self.max_retries = max(0, int(max_retries))
        self.retry_base_seconds = max(0.0, float(retry_base_seconds))
        self.max_retry_after_seconds = max(0.0, float(max_retry_after_seconds))
        self.max_market_age_seconds = max(0.0, float(max_market_age_seconds))
        self._sleep = sleep or asyncio.sleep
        self._jitter = jitter or (lambda delay: random.uniform(0.0, delay * 0.1))
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._market_cache: dict | None = None

    def _now(self) -> datetime:
        value = self._clock()
        parsed = _parse_timestamp(value)
        return parsed or datetime.now(timezone.utc)

    def _new_result(
        self,
        status: str,
        projection: list[float] | None,
        confidence: float | None,
        sources_used: list[str],
        sources_failed: list[dict],
        is_calibrated: bool,
        is_fallback: bool,
        warnings: list[str],
        request_id: str,
        timestamp: datetime,
        source_timestamps: dict | None = None,
    ) -> dict:
        return {
            "status": status,
            "projection": projection,
            "confidence": confidence,
            "sources_used": sources_used,
            "sources_failed": sources_failed,
            "is_calibrated": is_calibrated,
            "is_fallback": is_fallback,
            "warnings": warnings,
            "timestamp": _utc_iso(timestamp),
            "request_id": request_id,
            "source_timestamps": source_timestamps or {},
        }

    async def _request_with_retries(self, source: str, operation, **kwargs) -> dict:
        last_failure = {"status": False, "message": "Provider request failed"}
        transient_codes = {408, 425, 429, 500, 502, 503, 504}
        for attempt in range(self.max_retries + 1):
            try:
                response = await asyncio.wait_for(
                    asyncio.to_thread(operation, **kwargs),
                    timeout=self.request_timeout_seconds,
                )
            except asyncio.TimeoutError:
                last_failure = {"status": False, "message": f"{source} request timed out"}
                retryable = True
            except (ConnectionError, OSError) as exc:
                last_failure = {"status": False, "message": str(exc)}
                retryable = True
            except Exception as exc:
                return {"status": False, "message": f"{source} unexpected error: {exc}"}
            else:
                if isinstance(response, dict) and response.get("status") is True:
                    return response
                last_failure = (
                    response
                    if isinstance(response, dict)
                    else {
                        "status": False,
                        "message": f"{source} returned malformed data",
                    }
                )
                code = _status_code(last_failure)
                retryable = code in transient_codes
                if last_failure.get("malformed_json"):
                    retryable = False

            if not retryable or attempt >= self.max_retries:
                break
            now = self._now()
            retry_after = _retry_after_seconds(last_failure, now)
            if retry_after is not None and retry_after > self.max_retry_after_seconds:
                break
            base = min(self.retry_base_seconds * (2**attempt), self.max_retry_after_seconds)
            delay = min(base + max(0.0, float(self._jitter(base))), self.max_retry_after_seconds)
            if retry_after is not None:
                delay = max(delay, retry_after)
            await self._sleep(delay)
        return last_failure

    async def fetch_understat_xg(self, event_id: str | None) -> dict:
        if not event_id:
            return {"status": False, "data": None, "message": "Understat event_id was not provided"}
        return await self._request_with_retries("Understat", football.get_event_xg, event_id=event_id)

    async def fetch_polymarket_markets(self, sport: str, team_home: str, team_away: str) -> dict:
        return await self._request_with_retries(
            "Polymarket",
            polymarket.search_markets,
            query=f"{team_away} {team_home}",
            sport=sport,
            sports_market_types="moneyline",
            limit=100,
        )

    def _market_projection(self, response: Any, team_home: str, team_away: str, now: datetime):
        if not isinstance(response, dict) or response.get("status") is not True:
            return None, None, None, _failure("polymarket", response, "Polymarket request failed")
        payload = response.get("data")
        markets = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(markets, list) or not markets:
            return None, None, None, {"source": "polymarket", "reason": "No market data returned"}
        last_reason = "No market had explicit outcomes for both teams"
        for market in markets:
            if not isinstance(market, dict):
                last_reason = "Market response contained a malformed entry"
                continue
            home = _outcome_price(market, team_home)
            away = _outcome_price(market, team_away)
            if home is None or away is None:
                last_reason = "Market did not contain valid prices for both team labels"
                continue
            vector, normalized = _validated_probability_vector([home, away])
            if vector is None:
                last_reason = "Market probabilities were non-finite, out of range, or did not sum to one"
                continue
            source_time = _parse_timestamp(market.get("updated_at") or market.get("updatedAt"))
            if source_time is None:
                last_reason = "Market has no valid source timestamp"
                continue
            if source_time > now + timedelta(minutes=2):
                last_reason = "Market timestamp is in the future"
                continue
            if (now - source_time).total_seconds() > self.max_market_age_seconds:
                last_reason = "Market data is stale"
                continue
            warnings = []
            if normalized:
                warnings.append("Market probabilities were normalized for a small rounding discrepancy.")
            return vector, source_time, warnings, None
        return None, None, None, {"source": "polymarket", "reason": last_reason}

    def _read_cache(self, now: datetime):
        cached = self._market_cache
        if not isinstance(cached, dict):
            return None, "No cached projection is available"
        source_time = _parse_timestamp(cached.get("timestamp"))
        vector, _ = _validated_probability_vector(cached.get("projection"))
        if source_time is None or vector is None:
            return None, "Cached projection is malformed"
        age = (now - source_time).total_seconds()
        if age < -120:
            return None, "Cached projection timestamp is in the future"
        if age > self.max_market_age_seconds:
            return None, "Cached projection is stale"
        return {"projection": vector, "timestamp": source_time}, None

    def calculate_fallback_projection(
        self,
        team_home: str,
        team_away: str,
        reason: str,
        historical_data: dict | None = None,
        *,
        sources_failed: list[dict] | None = None,
        request_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> dict:
        """Use a Laplace-smoothed historical binary win rate, else return unavailable.

        Historical input must contain an ISO timestamp and an ``outcomes`` list with
        ``home``, ``away``, or ``draw`` entries. Draws count as half a win per side;
        a Beta(1, 1) prior prevents a zero/one estimate from a small sample.
        """
        now = timestamp or self._now()
        request_id = request_id or uuid.uuid4().hex
        failures = list(sources_failed or [])
        vector, history_timestamp, history_error = _historical_vector(historical_data, now)
        if vector is not None:
            return self._new_result(
                "degraded",
                vector,
                None,
                ["historical"],
                failures,
                False,
                True,
                [
                    f"Fallback uses a Beta(1,1) Laplace-smoothed historical binary outcome rate from {len(historical_data['outcomes'])} observations.",
                    "Historical estimate is not calibrated; confidence is unavailable.",
                    reason,
                ],
                request_id,
                now,
                {"historical": history_timestamp},
            )
        warnings = [reason]
        if history_error:
            warnings.append(history_error)
            failures.append({"source": "historical", "reason": history_error})
        warnings.append("No usable historical data exists; no numerical projection was fabricated.")
        return self._new_result(
            "unavailable",
            None,
            None,
            [],
            failures,
            False,
            False,
            warnings,
            request_id,
            now,
        )

    async def _safe_source(self, awaitable, source: str) -> dict:
        try:
            return await asyncio.wait_for(awaitable, timeout=self.request_timeout_seconds)
        except asyncio.TimeoutError:
            return {"status": False, "message": f"{source} request timed out"}
        except Exception as exc:
            return {"status": False, "message": f"{source} unexpected error: {exc}"}

    async def generate_projection_result(
        self,
        sport: str,
        team_home: str,
        team_away: str,
        understat_event_id: str | None = None,
        historical_data: dict | None = None,
    ) -> dict:
        """Return status/projection/confidence/source/fallback fields with UTC trace metadata.

        ``projection`` is a validated binary probability vector or ``None``.
        Confidence remains ``None`` unless a calibrated source is introduced.
        """
        now = self._now()
        request_id = uuid.uuid4().hex
        if not all(isinstance(value, str) for value in (sport, team_home, team_away)):
            return self._new_result(
                "unavailable",
                None,
                None,
                [],
                [],
                False,
                False,
                ["sport, team_home, and team_away must be strings"],
                request_id,
                now,
            )
        sport = sport.strip().lower()
        team_home = team_home.strip()
        team_away = team_away.strip()
        if not sport or not team_home or not team_away:
            return self._new_result(
                "unavailable",
                None,
                None,
                [],
                [],
                False,
                False,
                ["sport, team_home, and team_away are required"],
                request_id,
                now,
            )

        understat_task = self.fetch_understat_xg(understat_event_id) if understat_event_id else None
        understat_result, market_result = await asyncio.gather(
            self._safe_source(understat_task, "Understat") if understat_task else asyncio.sleep(0, result=None),
            self._safe_source(self.fetch_polymarket_markets(sport, team_home, team_away), "Polymarket"),
        )
        failures = []
        warnings = []
        source_timestamps = {}
        understat_valid = False
        if understat_event_id:
            understat_valid, understat_error = _valid_understat_result(understat_result)
            if not understat_valid:
                failures.append(_failure("understat", understat_result, understat_error or "Understat data invalid"))
            else:
                warnings.append(
                    "Understat xG is supporting context only; no validated conversion to win probabilities is applied."
                )
        else:
            warnings.append("Understat was not queried because no event_id was supplied.")

        vector, market_timestamp, market_warnings, market_error = self._market_projection(
            market_result, team_home, team_away, now
        )
        if market_error:
            failures.append(market_error)
            warnings.append(market_error["reason"])
        if vector is not None:
            source_timestamps["polymarket"] = _utc_iso(market_timestamp)
            warnings.extend(market_warnings or [])
            self._market_cache = {"projection": vector, "timestamp": _utc_iso(market_timestamp)}
            if not understat_valid and understat_event_id:
                warnings.append("Projection uses Polymarket alone because Understat was unavailable or invalid.")
            return self._new_result(
                "degraded" if failures else "ok",
                vector,
                None,
                ["polymarket"],
                failures,
                False,
                False,
                warnings + ["Market-implied probabilities are not an independently calibrated forecast."],
                request_id,
                now,
                source_timestamps,
            )

        cached, cache_error = self._read_cache(now)
        if cached is not None:
            warnings.append(
                f"Using locally cached Polymarket data from {_utc_iso(cached['timestamp'])}; live verification failed."
            )
            return self._new_result(
                "degraded",
                cached["projection"],
                None,
                ["local_cache"],
                failures,
                False,
                True,
                warnings,
                request_id,
                now,
                {"polymarket_cache": _utc_iso(cached["timestamp"])},
            )
        if cache_error and "No cached" not in cache_error:
            warnings.append(cache_error)
            failures.append({"source": "local_cache", "reason": cache_error})

        reason = "No fresh validated market projection is available"
        try:
            fallback = self.calculate_fallback_projection(
                team_home,
                team_away,
                reason,
                historical_data=historical_data,
                sources_failed=failures,
                request_id=request_id,
                timestamp=now,
            )
        except Exception as exc:
            warnings.append(f"Fallback calculation failed: {exc}")
            return self._new_result("unavailable", None, None, [], failures, False, False, warnings, request_id, now)
        fallback["warnings"] = warnings + fallback["warnings"]
        return fallback

    async def generate_unified_projection(
        self,
        sport: str,
        team_home: str,
        team_away: str,
        understat_event_id: str | None = None,
        historical_data: dict | None = None,
    ) -> dict:
        """Backward-compatible bool/data envelope around the canonical result."""
        result = await self.generate_projection_result(
            sport,
            team_home,
            team_away,
            understat_event_id=understat_event_id,
            historical_data=historical_data,
        )
        legacy_data = dict(result)
        legacy_data["projection_status"] = result["status"]
        legacy_data["output_vector"] = result["projection"]
        legacy_data["confidence_score"] = result["confidence"]
        message = "Projection available" if result["projection"] is not None else "Projection unavailable"
        return {"status": result["status"] != "unavailable", "data": legacy_data, "message": message}
