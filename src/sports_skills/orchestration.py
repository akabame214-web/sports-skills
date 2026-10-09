"""Small async adapter for read-only, market-implied sports context."""

from __future__ import annotations

import asyncio
import math
import re

from sports_skills import polymarket


def _team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _outcome_price(market: dict, team: str) -> float | None:
    team_key = _team_key(team)
    for outcome in market.get("outcomes", []):
        if not isinstance(outcome, dict) or _team_key(str(outcome.get("name", ""))) != team_key:
            continue
        try:
            price = float(outcome.get("price"))
        except (TypeError, ValueError):
            return None
        return price if math.isfinite(price) and 0 <= price <= 1 else None
    return None


class SportsSkillsOrchestrator:
    """Compose supported sports providers without inventing missing projections."""

    async def generate_unified_projection(
        self,
        sport: str,
        team_home: str,
        team_away: str,
    ) -> dict:
        """Return market-implied outcome prices for a matchup, when explicitly labeled.

        This does not calculate an independent statistical projection. Polymarket is
        queried through the repository's read-only connector in a worker thread so
        this async adapter does not block the event loop.
        """
        sport = sport.strip().lower()
        team_home = team_home.strip()
        team_away = team_away.strip()
        if not sport or not team_home or not team_away:
            return {
                "status": False,
                "data": None,
                "message": "sport, team_home, and team_away are required",
            }

        try:
            response = await asyncio.to_thread(
                polymarket.search_markets,
                query=f"{team_away} {team_home}",
                sport=sport,
                sports_market_types="moneyline",
                limit=100,
            )
        except Exception as exc:
            return {
                "status": False,
                "data": None,
                "message": f"Polymarket lookup failed: {exc}",
            }

        if not isinstance(response, dict) or not response.get("status"):
            message = response.get("message", "Invalid response") if isinstance(response, dict) else "Invalid response"
            return {"status": False, "data": None, "message": f"Polymarket lookup failed: {message}"}

        payload = response.get("data")
        markets = payload.get("markets", []) if isinstance(payload, dict) else []
        for market in markets:
            if not isinstance(market, dict):
                continue
            home_price = _outcome_price(market, team_home)
            away_price = _outcome_price(market, team_away)
            if home_price is None or away_price is None:
                continue

            favored = team_home if home_price > away_price else team_away if away_price > home_price else None
            return {
                "status": True,
                "data": {
                    "sport": sport,
                    "matchup": f"{team_away} @ {team_home}",
                    "market": {
                        "provider": "Polymarket",
                        "question": market.get("question", ""),
                        "outcomes": {
                            team_home: home_price,
                            team_away: away_price,
                        },
                        "favored_outcome": favored,
                        "updated_at": market.get("updated_at") or None,
                        "liquidity": market.get("liquidity"),
                    },
                    "independent_projection": None,
                    "confidence": None,
                    "caveats": [
                        "Prices are market-implied probabilities, not an independent model projection.",
                        "Market liquidity and the provider update time may be limited or stale.",
                        "No Action Network, Understat, injury, or historical-model data was used.",
                    ],
                },
                "message": "Market outcome prices retrieved",
            }

        return {
            "status": True,
            "data": {
                "sport": sport,
                "matchup": f"{team_away} @ {team_home}",
                "market": None,
                "independent_projection": None,
                "confidence": None,
                "caveats": [
                    "No matching market with explicit outcomes for both teams was found.",
                    "No fallback probability or projection was fabricated.",
                ],
            },
            "message": "No labeled moneyline market found",
        }
