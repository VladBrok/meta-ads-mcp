#!/usr/bin/env python3
"""
Unit tests for the automatic bid_strategy defaulting in create_campaign and create_adset.

When a campaign budget is provided without an explicit bid_strategy (and no bid cap), Meta defaults
the campaign to LOWEST_COST_WITH_BID_CAP, which requires a bid_amount/bid_cap and 400s if absent.
create_campaign now defaults such calls to LOWEST_COST_WITHOUT_CAP (automatic bidding) so the create
call succeeds, while leaving explicit strategies and cap-bidding callers untouched. Ad sets carry no
budget, so create_adset never defaults a bid strategy.

Usage:
    uv run python -m pytest tests/test_bid_strategy_default.py -v
"""

import inspect
import json
import pytest
from unittest.mock import patch

from meta_ads_mcp.core.campaigns import create_campaign
from meta_ads_mcp.core.adsets import create_adset


@pytest.fixture
def mock_auth_manager():
    with patch('meta_ads_mcp.core.api.auth_manager') as mock, \
         patch('meta_ads_mcp.core.api.get_current_access_token') as mock_get_token:
        mock.is_token_valid.return_value = True
        mock.app_id = "test_app_id"
        mock_get_token.return_value = "test_access_token"
        yield mock


def _sent_params(mock_api_request):
    """The params dict passed to make_api_request (3rd positional arg)."""
    return mock_api_request.call_args[0][2]


class TestCreateCampaignBidStrategyDefault:
    @pytest.fixture
    def mock_api_request(self):
        with patch('meta_ads_mcp.core.campaigns.make_api_request') as mock:
            mock.return_value = {"id": "campaign_123", "name": "Test Campaign"}
            yield mock

    @pytest.fixture
    def base_params(self):
        return {
            "access_token": "test_access_token",
            "account_id": "act_123456789",
            "name": "Test Campaign",
            "objective": "OUTCOME_TRAFFIC",
            "special_ad_categories": ["EMPLOYMENT"],
        }

    @pytest.mark.asyncio
    async def test_campaign_budget_without_strategy_defaults_to_lowest_cost(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """CBO budget + no bid_strategy/bid_cap -> LOWEST_COST_WITHOUT_CAP is sent."""
        await create_campaign(**base_params, daily_budget=100)
        params = _sent_params(mock_api_request)
        assert params["bid_strategy"] == "LOWEST_COST_WITHOUT_CAP"

    @pytest.mark.asyncio
    async def test_no_campaign_budget_does_not_add_strategy(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """No campaign-level budget (ad-set budget flow) -> bid_strategy not defaulted."""
        await create_campaign(**base_params)
        params = _sent_params(mock_api_request)
        assert "bid_strategy" not in params

    @pytest.mark.asyncio
    async def test_explicit_strategy_is_respected(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """Explicit bid_strategy is never overridden by the default."""
        await create_campaign(
            **base_params,
            daily_budget=100,
            bid_strategy="LOWEST_COST_WITH_BID_CAP",
            bid_cap=500,
        )
        params = _sent_params(mock_api_request)
        assert params["bid_strategy"] == "LOWEST_COST_WITH_BID_CAP"

    @pytest.mark.asyncio
    async def test_bid_cap_without_strategy_is_not_overridden(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """A caller passing a bid_cap keeps cap bidding -> no forced LOWEST_COST_WITHOUT_CAP."""
        await create_campaign(**base_params, daily_budget=100, bid_cap=500)
        params = _sent_params(mock_api_request)
        assert "bid_strategy" not in params
        assert params["bid_cap"] == "500"


class TestCreateAdsetBidStrategyDefault:
    @pytest.fixture
    def mock_api_request(self):
        with patch('meta_ads_mcp.core.adsets.make_api_request') as mock:
            mock.return_value = {"id": "adset_123"}
            yield mock

    @pytest.fixture
    def base_params(self):
        return {
            "access_token": "test_access_token",
            "account_id": "act_123456789",
            "campaign_id": "campaign_123456789",
            "name": "AdSet_1",
            "optimization_goal": "LANDING_PAGE_VIEWS",
            "billing_event": "IMPRESSIONS",
            "targeting": {"age_min": 18, "age_max": 65, "geo_locations": {"countries": ["NL"]}},
        }

    @pytest.mark.asyncio
    async def test_no_strategy_is_not_defaulted(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """The budget lives on the campaign, so no bid_strategy is defaulted at ad-set level."""
        await create_adset(**base_params)
        params = _sent_params(mock_api_request)
        assert "bid_strategy" not in params

    @pytest.mark.asyncio
    async def test_explicit_strategy_is_passed_through(
        self, mock_api_request, mock_auth_manager, base_params
    ):
        """Explicit bid_strategy reaches the API unchanged."""
        await create_adset(**base_params, bid_strategy="COST_CAP", bid_amount=200)
        params = _sent_params(mock_api_request)
        assert params["bid_strategy"] == "COST_CAP"
        assert params["bid_amount"] == "200"

    @pytest.mark.parametrize("budget_param", ["daily_budget", "lifetime_budget"])
    def test_adset_does_not_accept_a_budget(self, budget_param):
        """Budgets are campaign-level only -- create_adset must not expose them."""
        assert budget_param not in inspect.signature(create_adset).parameters
