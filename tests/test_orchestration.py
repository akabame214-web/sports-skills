import asyncio

from sports_skills import orchestration


def test_market_outcomes_are_matched_by_team_label(monkeypatch):
    monkeypatch.setattr(
        orchestration.polymarket,
        "search_markets",
        lambda **kwargs: {
            "status": True,
            "data": {
                "markets": [
                    {
                        "question": "Miami Heat vs Boston Celtics",
                        "outcomes": [
                            {"name": "Miami Heat", "price": 0.42},
                            {"name": "Boston Celtics", "price": 0.58},
                        ],
                        "updated_at": "2026-10-09T12:00:00Z",
                        "liquidity": 1200,
                    }
                ]
            },
            "message": "Found 1 markets",
        },
    )

    result = asyncio.run(
        orchestration.SportsSkillsOrchestrator().generate_unified_projection("NBA", "Boston Celtics", "Miami Heat")
    )

    assert result["status"] is True
    assert result["data"]["market"]["outcomes"] == {
        "Boston Celtics": 0.58,
        "Miami Heat": 0.42,
    }
    assert result["data"]["market"]["favored_outcome"] == "Boston Celtics"
    assert result["data"]["independent_projection"] is None


def test_unlabeled_market_does_not_create_a_fallback_projection(monkeypatch):
    monkeypatch.setattr(
        orchestration.polymarket,
        "search_markets",
        lambda **kwargs: {
            "status": True,
            "data": {"markets": [{"outcomes": [{"name": "Yes", "price": 0.7}]}]},
            "message": "Found 1 markets",
        },
    )

    result = asyncio.run(
        orchestration.SportsSkillsOrchestrator().generate_unified_projection("nba", "Boston Celtics", "Miami Heat")
    )

    assert result["status"] is True
    assert result["data"]["market"] is None
    assert result["data"]["independent_projection"] is None
    assert "No fallback probability" in result["data"]["caveats"][1]


def test_provider_error_is_reported(monkeypatch):
    monkeypatch.setattr(
        orchestration.polymarket,
        "search_markets",
        lambda **kwargs: {"status": False, "message": "offline"},
    )

    result = asyncio.run(
        orchestration.SportsSkillsOrchestrator().generate_unified_projection("nba", "Boston Celtics", "Miami Heat")
    )

    assert result["status"] is False
    assert "offline" in result["message"]
