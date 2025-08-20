"""
Tests for the get_account_pixels function.
"""

import pytest
import json
from unittest.mock import AsyncMock, patch
from meta_ads_mcp.core.ads import get_account_pixels


class TestGetAccountPixels:
    """Test the get_account_pixels function."""
    
    @pytest.mark.asyncio
    async def test_get_account_pixels_success(self):
        """Test successful pixel retrieval."""
        mock_pixels_response = {
            "data": [
                {
                    "id": "972201676202671",
                    "name": "Website Pixel",
                    "last_fired_time": "2024-01-15T10:30:00+0000"
                }
            ],
            "paging": {
                "cursors": {
                    "before": "OTcyMjAxNjc2MjAyNjcx",
                    "after": "OTcyMjAxNjc2MjAyNjcx"
                }
            }
        }
        
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.return_value = mock_pixels_response
            
            result = await get_account_pixels(account_id="act_348997869135954")
            result_data = json.loads(result)
            
            # Verify the API was called correctly
            mock_api.assert_called_once_with(
                "act_348997869135954/adspixels",
                "test_access_token",
                {"fields": "id,name,last_fired_time"}
            )
            
            # Verify response structure
            assert "data" in result_data
            assert len(result_data["data"]) == 1
            assert result_data["data"][0]["id"] == "972201676202671"
            assert result_data["data"][0]["name"] == "Website Pixel"
            assert "paging" in result_data

    @pytest.mark.asyncio
    async def test_get_account_pixels_no_account_id(self):
        """Test error when no account ID is provided."""
        result = await get_account_pixels(account_id=None)
        result_data = json.loads(result)
        
        # Handle MCP response format - error might be wrapped in "data" field
        if "data" in result_data and isinstance(result_data["data"], str):
            error_data = json.loads(result_data["data"])
            assert "error" in error_data
            assert "No account ID provided" in error_data["error"]
        else:
            assert "error" in result_data
            assert "No account ID provided" in result_data["error"]

    @pytest.mark.asyncio
    async def test_get_account_pixels_empty_account_id(self):
        """Test error when empty account ID is provided."""
        result = await get_account_pixels(account_id="")
        result_data = json.loads(result)
        
        # Handle MCP response format - error might be wrapped in "data" field
        if "data" in result_data and isinstance(result_data["data"], str):
            error_data = json.loads(result_data["data"])
            assert "error" in error_data
            assert "No account ID provided" in error_data["error"]
        else:
            assert "error" in result_data
            assert "No account ID provided" in result_data["error"]

    @pytest.mark.asyncio
    async def test_get_account_pixels_act_prefix_handling(self):
        """Test that account IDs without 'act_' prefix are handled correctly."""
        mock_pixels_response = {
            "data": [],
            "paging": {}
        }
        
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.return_value = mock_pixels_response
            
            # Test with account ID without 'act_' prefix
            result = await get_account_pixels(account_id="348997869135954")
            
            # Check that API call was made with 'act_' prefix added
            mock_api.assert_called_once_with(
                "act_348997869135954/adspixels",
                "test_access_token",
                {"fields": "id,name,last_fired_time"}
            )

    @pytest.mark.asyncio
    async def test_get_account_pixels_with_act_prefix(self):
        """Test that account IDs with 'act_' prefix are preserved."""
        mock_pixels_response = {
            "data": [],
            "paging": {}
        }
        
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.return_value = mock_pixels_response
            
            # Test with account ID with 'act_' prefix
            result = await get_account_pixels(account_id="act_348997869135954")
            
            # Check that API call was made with existing 'act_' prefix
            mock_api.assert_called_once_with(
                "act_348997869135954/adspixels",
                "test_access_token",
                {"fields": "id,name,last_fired_time"}
            )

    @pytest.mark.asyncio
    async def test_get_account_pixels_empty_response(self):
        """Test when no pixels are found for the account."""
        mock_empty_response = {
            "data": [],
            "paging": {}
        }
        
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.return_value = mock_empty_response
            
            result = await get_account_pixels(account_id="act_348997869135954")
            result_data = json.loads(result)
            
            # Should return empty data array
            assert "data" in result_data
            assert len(result_data["data"]) == 0

    @pytest.mark.asyncio
    async def test_get_account_pixels_api_error(self):
        """Test error handling when API call fails."""
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.side_effect = Exception("API Error")
            
            result = await get_account_pixels(account_id="act_348997869135954")
            result_data = json.loads(result)
            
            # Handle MCP response format - error might be wrapped in "data" field
            if "data" in result_data and isinstance(result_data["data"], str):
                error_data = json.loads(result_data["data"])
                assert "error" in error_data
                assert "Failed to get account pixels" in error_data["error"]
                assert "details" in error_data
                assert "API Error" in error_data["details"]
            else:
                # Should return error response
                assert "error" in result_data
                assert "Failed to get account pixels" in result_data["error"]
                assert "details" in result_data
                assert "API Error" in result_data["details"]

    @pytest.mark.asyncio
    async def test_get_account_pixels_multiple_pixels(self):
        """Test response with multiple pixels."""
        mock_pixels_response = {
            "data": [
                {
                    "id": "972201676202671",
                    "name": "Website Pixel",
                    "last_fired_time": "2024-01-15T10:30:00+0000"
                },
                {
                    "id": "972201676202672",
                    "name": "App Pixel",
                    "last_fired_time": "2024-01-14T15:45:00+0000"
                }
            ],
            "paging": {
                "cursors": {
                    "before": "OTcyMjAxNjc2MjAyNjcx",
                    "after": "OTcyMjAxNjc2MjAyNjcy"
                }
            }
        }
        
        with patch('meta_ads_mcp.core.api.get_current_access_token', new_callable=AsyncMock) as mock_auth, \
             patch('meta_ads_mcp.core.ads.make_api_request') as mock_api:
            
            mock_auth.return_value = "test_access_token"
            mock_api.return_value = mock_pixels_response
            
            result = await get_account_pixels(account_id="act_348997869135954")
            result_data = json.loads(result)
            
            # Verify multiple pixels are returned
            assert "data" in result_data
            assert len(result_data["data"]) == 2
            
            # Check pixel IDs
            pixel_ids = [pixel["id"] for pixel in result_data["data"]]
            assert "972201676202671" in pixel_ids
            assert "972201676202672" in pixel_ids
            
            # Check pixel names
            pixel_names = [pixel["name"] for pixel in result_data["data"]]
            assert "Website Pixel" in pixel_names
            assert "App Pixel" in pixel_names


if __name__ == "__main__":
    pytest.main([__file__])