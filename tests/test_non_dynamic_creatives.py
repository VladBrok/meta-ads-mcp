"""Tests for non-dynamic creative features.

Tests for the adjusted create_ad_creative function that supports only non-dynamic creatives:
- Single headline
- Single description  
- Traditional object_story_spec format
"""

import pytest
import json
from unittest.mock import AsyncMock, patch
from meta_ads_mcp.core.ads import create_ad_creative, update_ad_creative


@pytest.mark.asyncio
class TestNonDynamicCreatives:
    """Test cases for non-dynamic creative features."""
    
    async def test_create_non_dynamic_creative_basic(self):
        """Test creating a basic non-dynamic ad creative."""
        
        sample_creative_data = {
            "id": "123456789",
            "name": "Test Creative",
            "status": "ACTIVE"
        }
        
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = sample_creative_data
            
            result = await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="Test Creative",
                image_hash="abc123",
                page_id="987654321",
                link_url="https://example.com",
                message="Test message",
                headline="Single Headline",
                description="Single Description",
                call_to_action_type="LEARN_MORE"
            )
            
            result_data = json.loads(result)
            assert result_data["success"] is True
            
            # Verify the API call was made with object_story_spec (not asset_feed_spec)
            call_args_list = mock_api.call_args_list
            assert len(call_args_list) >= 1
            
            # First call should be the creative creation
            first_call = call_args_list[0]
            creative_data = first_call[0][2]  # params is the third argument
            
            # Should use object_story_spec for non-dynamic creative
            assert "object_story_spec" in creative_data
            assert "asset_feed_spec" not in creative_data
            
            # Verify the structure
            link_data = creative_data["object_story_spec"]["link_data"]
            assert link_data["image_hash"] == "abc123"
            assert link_data["link"] == "https://example.com"
            assert link_data["message"] == "Test message"
            assert link_data["name"] == "Single Headline"
            assert link_data["description"] == "Single Description"
            assert link_data["call_to_action"]["type"] == "LEARN_MORE"
    
    async def test_create_non_dynamic_creative_minimal(self):
        """Test creating a minimal non-dynamic ad creative."""
        
        sample_creative_data = {
            "id": "123456789",
            "name": "Minimal Creative"
        }
        
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = sample_creative_data
            
            result = await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="Minimal Creative",
                image_hash="abc123",
                page_id="987654321"
            )
            
            result_data = json.loads(result)
            assert result_data["success"] is True
            
            # Verify minimal structure
            call_args_list = mock_api.call_args_list
            first_call = call_args_list[0]
            creative_data = first_call[0][2]
            
            assert "object_story_spec" in creative_data
            assert "asset_feed_spec" not in creative_data
            
            link_data = creative_data["object_story_spec"]["link_data"]
            assert link_data["image_hash"] == "abc123"
            assert link_data["link"] == "https://facebook.com"  # default
    
    async def test_update_non_dynamic_creative(self):
        """Test updating ad creative metadata (name and status only).

        Note: According to Meta API docs, creative content (message, headline, description, images)
        is immutable. Only metadata (name, status, account_id) can be updated.
        """

        sample_update_response = {
            "success": True,
            "id": "123456789"
        }

        sample_creative_details = {
            "id": "123456789",
            "name": "Updated Creative Name",
            "status": "ACTIVE"
        }

        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.side_effect = [sample_update_response, sample_creative_details]

            result = await update_ad_creative(
                access_token="test_token",
                creative_id="123456789",
                name="Updated Creative Name",
                status="ACTIVE"
            )

            result_data = json.loads(result)
            assert result_data["success"] is True
            assert result_data["creative_id"] == "123456789"

            call_args_list = mock_api.call_args_list
            first_call = call_args_list[0]
            update_data = first_call[0][2]

            assert update_data["name"] == "Updated Creative Name"
            assert update_data["status"] == "ACTIVE"
            assert "object_story_spec" not in update_data
            assert "message" not in update_data
            assert "headline" not in update_data
    
    async def test_create_creative_missing_required_params(self):
        """Test error handling for missing required parameters."""
        
        # Test missing account_id
        result = await create_ad_creative(
            access_token="test_token",
            name="Test Creative",
            image_hash="abc123"
        )
        result_data = json.loads(result)
        # Error might be wrapped in "data" field due to MCP decorator
        if "data" in result_data:
            error_data = json.loads(result_data["data"])
            assert "error" in error_data
            assert "account id" in error_data["error"].lower()
        else:
            assert "error" in result_data
            assert "account id" in result_data["error"].lower()
        
        # Test missing image_hash
        result = await create_ad_creative(
            access_token="test_token",
            account_id="act_123456789",
            name="Test Creative"
        )
        result_data = json.loads(result)
        # Error might be wrapped in "data" field due to MCP decorator
        if "data" in result_data:
            error_data = json.loads(result_data["data"])
            assert "error" in error_data
            assert "image_hash" in error_data["error"].lower()
        else:
            assert "error" in result_data
            assert "image_hash" in result_data["error"].lower()
    
    async def test_update_creative_missing_creative_id(self):
        """Test error handling for missing creative ID in update."""

        result = await update_ad_creative(
            access_token="test_token",
            name="Updated Creative"
        )
        result_data = json.loads(result)
        # Error might be wrapped in "data" field due to MCP decorator
        if "data" in result_data:
            error_data = json.loads(result_data["data"])
            assert "error" in error_data
            assert "creative id" in error_data["error"].lower()
        else:
            assert "error" in result_data
            assert "creative id" in result_data["error"].lower()


def _unwrap_error(result: str) -> dict:
    """Unwrap a create_ad_creative error result, which the MCP decorator may nest under 'data'."""
    result_data = json.loads(result)
    if "data" in result_data:
        return json.loads(result_data["data"])
    return result_data


@pytest.mark.asyncio
class TestPlacementAssetCustomization:
    """Test cases for the placement asset customization (feed/story image pair) path."""

    async def test_pac_creative_builds_asset_feed_spec(self):
        """Both hashes -> asset_feed_spec with two labeled images and two placement rules."""
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"id": "cr_1"}

            result = await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="PAC Creative",
                page_id="987654321",
                link_url="https://example.com",
                message="Test message",
                headline="Single Headline",
                description="Single Description",
                call_to_action_type="LEARN_MORE",
                feed_image_hash="square_hash",
                story_image_hash="vertical_hash",
                publisher_platforms=["facebook", "instagram"],
            )

            result_data = json.loads(result)
            assert result_data["success"] is True
            assert result_data["creative_id"] == "cr_1"

            creative_data = mock_api.call_args_list[0][0][2]

            # object_story_spec carries only the page id (no link_data) alongside asset_feed_spec.
            assert creative_data["object_story_spec"] == {"page_id": "987654321"}
            assert "instagram_actor_id" not in creative_data

            afs = creative_data["asset_feed_spec"]
            assert afs["ad_formats"] == ["SINGLE_IMAGE"]
            assert afs["images"] == [
                {"hash": "square_hash", "adlabels": [{"name": "feed_img"}]},
                {"hash": "vertical_hash", "adlabels": [{"name": "story_img"}]},
            ]
            assert afs["bodies"] == [{"text": "Test message"}]
            assert afs["titles"] == [{"text": "Single Headline"}]
            assert afs["descriptions"] == [{"text": "Single Description"}]
            assert afs["link_urls"] == [{"website_url": "https://example.com"}]
            assert afs["call_to_action_types"] == ["LEARN_MORE"]

            # Story placements get the vertical image via an explicit rule; every other
            # placement gets the square image via the empty catch-all rule. is_default is
            # not set (it is a Multi-Language-ads-only field, not a placement fallback).
            rules = afs["asset_customization_rules"]
            assert len(rules) == 2
            story_rule, feed_rule = rules
            assert story_rule["image_label"] == {"name": "story_img"}
            assert story_rule["customization_spec"] == {
                "publisher_platforms": ["facebook", "instagram"],
                "facebook_positions": ["story"],
                "instagram_positions": ["story"],
            }
            assert feed_rule["image_label"] == {"name": "feed_img"}
            assert feed_rule["customization_spec"] == {}
            assert "is_default" not in feed_rule

    async def test_pac_fb_only_filters_instagram_positions(self):
        """Facebook-only ad set -> story rule carries facebook_positions only; empty catch-all stays."""
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"id": "cr_2"}

            await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="PAC FB",
                page_id="987654321",
                feed_image_hash="square_hash",
                story_image_hash="vertical_hash",
                publisher_platforms=["facebook"],
            )

            afs = mock_api.call_args_list[0][0][2]["asset_feed_spec"]
            story_rule, feed_rule = afs["asset_customization_rules"]
            assert story_rule["customization_spec"]["publisher_platforms"] == ["facebook"]
            assert story_rule["customization_spec"]["facebook_positions"] == ["story"]
            assert "instagram_positions" not in story_rule["customization_spec"]
            assert feed_rule["customization_spec"] == {}

    async def test_pac_ig_only_anchors_facebook(self):
        """Instagram-only ad set -> story rule still anchors Facebook so Meta resolves the Page's Instagram identity; empty catch-all stays."""
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"id": "cr_3"}

            await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="PAC IG",
                page_id="987654321",
                feed_image_hash="square_hash",
                story_image_hash="vertical_hash",
                publisher_platforms=["instagram"],
            )

            afs = mock_api.call_args_list[0][0][2]["asset_feed_spec"]
            story_rule, feed_rule = afs["asset_customization_rules"]
            assert story_rule["customization_spec"] == {
                "publisher_platforms": ["facebook", "instagram"],
                "facebook_positions": ["story"],
                "instagram_positions": ["story"],
            }
            assert feed_rule["customization_spec"] == {}

    async def test_pac_defaults_to_both_platforms(self):
        """Omitting publisher_platforms defaults the story rule to facebook + instagram."""
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"id": "cr_4"}

            await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="PAC default",
                page_id="987654321",
                feed_image_hash="square_hash",
                story_image_hash="vertical_hash",
            )

            afs = mock_api.call_args_list[0][0][2]["asset_feed_spec"]
            story_rule, feed_rule = afs["asset_customization_rules"]
            assert story_rule["customization_spec"]["publisher_platforms"] == ["facebook", "instagram"]
            assert feed_rule["customization_spec"] == {}
            assert "is_default" not in feed_rule

    async def test_pac_identical_hashes_collapse_to_single_image(self):
        """Identical feed/story hashes collapse to the classic single-image creative."""
        with patch('meta_ads_mcp.core.ads.make_api_request', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"id": "cr_5"}

            await create_ad_creative(
                access_token="test_token",
                account_id="act_123456789",
                name="PAC same",
                page_id="987654321",
                link_url="https://example.com",
                feed_image_hash="same_hash",
                story_image_hash="same_hash",
            )

            creative_data = mock_api.call_args_list[0][0][2]
            assert "asset_feed_spec" not in creative_data
            assert creative_data["object_story_spec"]["link_data"]["image_hash"] == "same_hash"

    async def test_pac_rejects_partial_pair(self):
        """Only one of the two hashes -> error asking for BOTH."""
        result = await create_ad_creative(
            access_token="test_token",
            account_id="act_123456789",
            name="PAC partial",
            page_id="987654321",
            feed_image_hash="square_hash",
        )
        error_data = _unwrap_error(result)
        assert "error" in error_data
        assert "both" in error_data["error"].lower()

    async def test_pac_rejects_with_video_id(self):
        """feed/story pair cannot combine with a video."""
        result = await create_ad_creative(
            access_token="test_token",
            account_id="act_123456789",
            name="PAC video",
            page_id="987654321",
            video_id="vid_1",
            feed_image_hash="square_hash",
            story_image_hash="vertical_hash",
        )
        error_data = _unwrap_error(result)
        assert "error" in error_data
        assert "video_id" in error_data["error"].lower()

    async def test_pac_rejects_with_image_hash(self):
        """feed/story pair cannot combine with a single image_hash."""
        result = await create_ad_creative(
            access_token="test_token",
            account_id="act_123456789",
            name="PAC single",
            page_id="987654321",
            image_hash="abc123",
            feed_image_hash="square_hash",
            story_image_hash="vertical_hash",
        )
        error_data = _unwrap_error(result)
        assert "error" in error_data
        assert "not both" in error_data["error"].lower()