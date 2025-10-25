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
            assert "image hash" in error_data["error"].lower()
        else:
            assert "error" in result_data
            assert "image hash" in result_data["error"].lower()
    
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