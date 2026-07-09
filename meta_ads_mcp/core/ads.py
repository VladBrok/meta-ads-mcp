"""Ad and Creative-related functionality for Meta Ads API."""

import json
from typing import Optional, Dict, Any, List
import io
from PIL import Image as PILImage
from mcp.server.fastmcp import Image
import os
import time

from .api import meta_api_tool, make_api_request, make_batch_api_request, make_api_request_with_file
from .accounts import get_ad_accounts
from .utils import download_image, try_multiple_download_methods, ad_creative_images, extract_creative_image_urls
from .server import mcp_server
from .ad_enums import AdStatus


@mcp_server.tool()
@meta_api_tool
async def get_ads(access_token: str = None, account_id: str = None, limit: int = 10, 
                 campaign_id: str = "", adset_id: str = "") -> str:
    """
    Get ads for a Meta Ads account with optional filtering.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of ads to return (default: 10)
        campaign_id: Optional campaign ID to filter by
        adset_id: Optional ad set ID to filter by
    """
    if not account_id:
        return json.dumps({"error": "No account ID specified"})
    
    # Prioritize adset_id over campaign_id - use adset-specific endpoint
    if adset_id:
        endpoint = f"{adset_id}/ads"
        params = {
            "fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs",
            "limit": limit
        }
    # Use campaign-specific endpoint if campaign_id is provided
    elif campaign_id:
        endpoint = f"{campaign_id}/ads"
        params = {
            "fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs",
            "limit": limit
        }
    else:
        # Default to account-level endpoint if no specific filters
        endpoint = f"{account_id}/ads"
        params = {
            "fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs",
            "limit": limit
        }

    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def get_ad_details(access_token: str = None, ad_id: str = None) -> str:
    """
    Get detailed information about a specific ad.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        ad_id: Meta Ads ad ID
    """
    if not ad_id:
        return json.dumps({"error": "No ad ID provided"})
        
    endpoint = f"{ad_id}"
    params = {
        "fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs,preview_shareable_link"
    }
    
    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def create_ad(
    account_id: str = None,
    name: str = None,
    adset_id: str = None,
    creative_id: str = None,
    status: AdStatus = AdStatus.PAUSED,
    bid_amount = None,
    tracking_specs: Optional[List[Dict[str, Any]]] = None,
    access_token: str = None
) -> str:
    """
    Create a new ad with an existing creative.
    
    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        name: Ad name
        adset_id: Ad set ID where this ad will be placed
        creative_id: ID of an existing creative to use
        status: Initial ad status (default: PAUSED)
        bid_amount: Optional bid amount in account currency (in cents)
        tracking_specs: Optional tracking specifications (e.g., for pixel events).
                      Example: [{"action.type":"offsite_conversion","fb_pixel":["YOUR_PIXEL_ID"]}]
        access_token: Meta API access token (optional - will use cached token if not provided)
    """
    # Check required parameters
    if not account_id:
        return json.dumps({"error": "No account ID provided"})
    
    if not name:
        return json.dumps({"error": "No ad name provided"})
    
    if not adset_id:
        return json.dumps({"error": "No ad set ID provided"})
    
    if not creative_id:
        return json.dumps({"error": "No creative ID provided"})
    
    endpoint = f"{account_id}/ads"
    
    params = {
        "name": name,
        "adset_id": adset_id,
        "creative": {"creative_id": creative_id},
        "status": getattr(status, 'value', status)
    }
    
    # Add bid amount if provided
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
        
    # Add tracking specs if provided
    if tracking_specs is not None:
        params["tracking_specs"] = json.dumps(tracking_specs) # Needs to be JSON encoded string
    
    try:
        data = await make_api_request(endpoint, access_token, params, method="POST")
        return json.dumps(data)
    except Exception as e:
        error_msg = str(e)
        return json.dumps({
            "error": "Failed to create ad",
            "details": error_msg,
            "params_sent": params
        })


@mcp_server.tool()
@meta_api_tool
async def get_ad_creatives(access_token: str = None, ad_id: str = None) -> str:
    """
    Get creative details for a specific ad. Best if combined with get_ad_image to get the full image.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        ad_id: Meta Ads ad ID (ID of an ad - NOT ad creative ID - providing an ad creative ID will result in an error)
    """
    if not ad_id:
        return json.dumps({"error": "No ad ID provided"})
        
    endpoint = f"{ad_id}/adcreatives"
    params = {
        "fields": "id,name,title,body,status,image_url,image_hash,object_story_spec,asset_feed_spec"
    }
    
    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def get_ad_image(access_token: str = None, ad_id: str = None) -> Image:
    """
    Get, download, and visualize a Meta ad image in one step. Useful to see the image in the LLM.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        ad_id: Meta Ads ad ID
    
    Returns:
        The ad image ready for direct visual analysis
    """
    if not ad_id:
        return "Error: No ad ID provided"
        
    print(f"Attempting to get and analyze creative image for ad {ad_id}")
    
    # First, get creative and account IDs
    ad_endpoint = f"{ad_id}"
    ad_params = {
        "fields": "creative{id},account_id"
    }
    
    ad_data = await make_api_request(ad_endpoint, access_token, ad_params)
    
    if "error" in ad_data:
        return f"Error: Could not get ad data - {json.dumps(ad_data)}"
    
    # Extract account_id
    account_id = ad_data.get("account_id", "")
    if not account_id:
        return "Error: No account ID found"
    
    # Extract creative ID
    if "creative" not in ad_data:
        return "Error: No creative found for this ad"
        
    creative_data = ad_data.get("creative", {})
    creative_id = creative_data.get("id")
    if not creative_id:
        return "Error: No creative ID found"
    
    # Get creative details to find image hash
    creative_endpoint = f"{creative_id}"
    creative_params = {
        "fields": "id,name,image_hash,asset_feed_spec"
    }
    
    creative_details = await make_api_request(creative_endpoint, access_token, creative_params)
    
    # Identify image hashes to use from creative
    image_hashes = []
    
    # Check for direct image_hash on creative
    if "image_hash" in creative_details:
        image_hashes.append(creative_details["image_hash"])
    
    # Check asset_feed_spec for image hashes - common in Advantage+ ads
    if "asset_feed_spec" in creative_details and "images" in creative_details["asset_feed_spec"]:
        for image in creative_details["asset_feed_spec"]["images"]:
            if "hash" in image:
                image_hashes.append(image["hash"])
    
    if not image_hashes:
        # If no hashes found, try to extract from the first creative we found in the API
        # and also check for direct URLs as fallback
        creative_json = await get_ad_creatives(access_token=access_token, ad_id=ad_id)
        creative_data = json.loads(creative_json)
        
        # Try to extract hash from data array
        if "data" in creative_data and creative_data["data"]:
            for creative in creative_data["data"]:
                # Check object_story_spec for image hash
                if "object_story_spec" in creative and "link_data" in creative["object_story_spec"]:
                    link_data = creative["object_story_spec"]["link_data"]
                    if "image_hash" in link_data:
                        image_hashes.append(link_data["image_hash"])
                # Check direct image_hash on creative
                elif "image_hash" in creative:
                    image_hashes.append(creative["image_hash"])
                # Check asset_feed_spec for image hashes
                elif "asset_feed_spec" in creative and "images" in creative["asset_feed_spec"]:
                    images = creative["asset_feed_spec"]["images"]
                    if images and len(images) > 0 and "hash" in images[0]:
                        image_hashes.append(images[0]["hash"])
        
        # If still no image hashes found, try direct URL fallback approach
        if not image_hashes:
            print("No image hashes found, trying direct URL fallback...")
            
            image_url = None
            if "data" in creative_data and creative_data["data"]:
                creative = creative_data["data"][0]
                
                # Prioritize higher quality image URLs in this order:
                # 1. image_urls_for_viewing (usually highest quality)
                # 2. image_url (direct field)
                # 3. object_story_spec.link_data.picture (usually full size)
                # 4. thumbnail_url (last resort - often profile thumbnail)
                
                if "image_urls_for_viewing" in creative and creative["image_urls_for_viewing"]:
                    image_url = creative["image_urls_for_viewing"][0]
                    print(f"Using image_urls_for_viewing: {image_url}")
                elif "image_url" in creative and creative["image_url"]:
                    image_url = creative["image_url"]
                    print(f"Using image_url: {image_url}")
                elif "object_story_spec" in creative and "link_data" in creative["object_story_spec"]:
                    link_data = creative["object_story_spec"]["link_data"]
                    if "picture" in link_data and link_data["picture"]:
                        image_url = link_data["picture"]
                        print(f"Using object_story_spec.link_data.picture: {image_url}")
                elif "thumbnail_url" in creative and creative["thumbnail_url"]:
                    image_url = creative["thumbnail_url"]
                    print(f"Using thumbnail_url (fallback): {image_url}")
            
            if not image_url:
                return "Error: No image URLs found in creative"
            
            # Download the image directly
            print(f"Downloading image from direct URL: {image_url}")
            image_bytes = await download_image(image_url)
            
            if not image_bytes:
                return "Error: Failed to download image from direct URL"
            
            try:
                # Convert bytes to PIL Image
                img = PILImage.open(io.BytesIO(image_bytes))
                
                # Convert to RGB if needed
                if img.mode != "RGB":
                    img = img.convert("RGB")
                    
                # Create a byte stream of the image data
                byte_arr = io.BytesIO()
                img.save(byte_arr, format="JPEG")
                img_bytes = byte_arr.getvalue()
                
                # Return as an Image object that LLM can directly analyze
                return Image(data=img_bytes, format="jpeg")
                
            except Exception as e:
                return f"Error processing image from direct URL: {str(e)}"
    
    print(f"Found image hashes: {image_hashes}")
    
    # Now fetch image data using adimages endpoint with specific format
    image_endpoint = f"act_{account_id}/adimages"
    
    # Format the hashes parameter exactly as in our successful curl test
    hashes_str = f'["{image_hashes[0]}"]'  # Format first hash only, as JSON string array
    
    image_params = {
        "fields": "hash,url,width,height,name,status",
        "hashes": hashes_str
    }
    
    print(f"Requesting image data with params: {image_params}")
    image_data = await make_api_request(image_endpoint, access_token, image_params)
    
    if "error" in image_data:
        return f"Error: Failed to get image data - {json.dumps(image_data)}"
    
    if "data" not in image_data or not image_data["data"]:
        return "Error: No image data returned from API"
    
    # Get the first image URL
    first_image = image_data["data"][0]
    image_url = first_image.get("url")
    
    if not image_url:
        return "Error: No valid image URL found"
    
    print(f"Downloading image from URL: {image_url}")
    
    # Download the image
    image_bytes = await download_image(image_url)
    
    if not image_bytes:
        return "Error: Failed to download image"
    
    try:
        # Convert bytes to PIL Image
        img = PILImage.open(io.BytesIO(image_bytes))
        
        # Convert to RGB if needed
        if img.mode != "RGB":
            img = img.convert("RGB")
            
        # Create a byte stream of the image data
        byte_arr = io.BytesIO()
        img.save(byte_arr, format="JPEG")
        img_bytes = byte_arr.getvalue()
        
        # Return as an Image object that LLM can directly analyze
        return Image(data=img_bytes, format="jpeg")
        
    except Exception as e:
        return f"Error processing image: {str(e)}"


@mcp_server.tool()
@meta_api_tool
async def save_ad_image_locally(access_token: str = None, ad_id: str = None, output_dir: str = "ad_images") -> str:
    """
    Get, download, and save a Meta ad image locally, returning the file path.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        ad_id: Meta Ads ad ID
        output_dir: Directory to save the image file (default: 'ad_images')
    
    Returns:
        The file path to the saved image, or an error message string.
    """
    if not ad_id:
        return json.dumps({"error": "No ad ID provided"})
        
    print(f"Attempting to get and save creative image for ad {ad_id}")
    
    # First, get creative and account IDs
    ad_endpoint = f"{ad_id}"
    ad_params = {
        "fields": "creative{id},account_id"
    }
    
    ad_data = await make_api_request(ad_endpoint, access_token, ad_params)
    
    if "error" in ad_data:
        return json.dumps({"error": f"Could not get ad data - {json.dumps(ad_data)}"})
    
    account_id = ad_data.get("account_id")
    if not account_id:
        return json.dumps({"error": "No account ID found for ad"})
    
    if "creative" not in ad_data:
        return json.dumps({"error": "No creative found for this ad"})
        
    creative_data = ad_data.get("creative", {})
    creative_id = creative_data.get("id")
    if not creative_id:
        return json.dumps({"error": "No creative ID found"})
    
    # Get creative details to find image hash
    creative_endpoint = f"{creative_id}"
    creative_params = {
        "fields": "id,name,image_hash,asset_feed_spec"
    }
    creative_details = await make_api_request(creative_endpoint, access_token, creative_params)
    
    image_hashes = []
    if "image_hash" in creative_details:
        image_hashes.append(creative_details["image_hash"])
    if "asset_feed_spec" in creative_details and "images" in creative_details["asset_feed_spec"]:
        for image in creative_details["asset_feed_spec"]["images"]:
            if "hash" in image:
                image_hashes.append(image["hash"])
    
    if not image_hashes:
        # Fallback attempt (as in get_ad_image)
        creative_json = await get_ad_creatives(ad_id=ad_id, access_token=access_token) # Ensure ad_id is passed correctly
        creative_data_list = json.loads(creative_json)
        if 'data' in creative_data_list and creative_data_list['data']:
             first_creative = creative_data_list['data'][0]
             if 'object_story_spec' in first_creative and 'link_data' in first_creative['object_story_spec'] and 'image_hash' in first_creative['object_story_spec']['link_data']:
                 image_hashes.append(first_creative['object_story_spec']['link_data']['image_hash'])
             elif 'image_hash' in first_creative: # Check direct hash on creative data
                  image_hashes.append(first_creative['image_hash'])


    if not image_hashes:
        return json.dumps({"error": "No image hashes found in creative or fallback"})

    print(f"Found image hashes: {image_hashes}")
    
    # Fetch image data using the first hash
    image_endpoint = f"act_{account_id}/adimages"
    hashes_str = f'["{image_hashes[0]}"]'
    image_params = {
        "fields": "hash,url,width,height,name,status",
        "hashes": hashes_str
    }
    
    print(f"Requesting image data with params: {image_params}")
    image_data = await make_api_request(image_endpoint, access_token, image_params)
    
    if "error" in image_data:
        return json.dumps({"error": f"Failed to get image data - {json.dumps(image_data)}"})
    
    if "data" not in image_data or not image_data["data"]:
        return json.dumps({"error": "No image data returned from API"})
        
    first_image = image_data["data"][0]
    image_url = first_image.get("url")
    
    if not image_url:
        return json.dumps({"error": "No valid image URL found in API response"})
        
    print(f"Downloading image from URL: {image_url}")
    
    # Download and Save Image
    image_bytes = await download_image(image_url)
    
    if not image_bytes:
        return json.dumps({"error": "Failed to download image"})
        
    try:
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Create a filename (e.g., using ad_id and image hash)
        file_extension = ".jpg" # Default extension, could try to infer from headers later
        filename = f"{ad_id}_{image_hashes[0]}{file_extension}"
        filepath = os.path.join(output_dir, filename)
        
        # Save the image bytes to the file
        with open(filepath, "wb") as f:
            f.write(image_bytes)
            
        print(f"Image saved successfully to: {filepath}")
        return json.dumps({"filepath": filepath}) # Return JSON with filepath

    except Exception as e:
        return json.dumps({"error": f"Failed to save image: {str(e)}"})


@mcp_server.tool()
@meta_api_tool
async def update_ad(
    ad_id: str,
    status: Optional[AdStatus] = None,
    bid_amount: int = None,
    tracking_specs = None,
    creative_id: str = None,
    access_token: str = None
) -> str:
    """
    Update an ad with new settings.
    
    Args:
        ad_id: Meta Ads ad ID
        status: Ad status
        bid_amount: Optional bid amount in account currency (in cents)
        tracking_specs: Optional tracking specifications (e.g., for pixel events).
                      Example: [{"action.type":"offsite_conversion","fb_pixel":["YOUR_PIXEL_ID"]}]
        creative_id: ID of the creative to associate with this ad (changes the ad's image/content)
        access_token: Meta API access token (optional - will use cached token if not provided)
    """
    if not ad_id:
        return json.dumps({"error": "Ad ID is required"})

    params = {}
    if status is not None:
        params["status"] = getattr(status, 'value', status)
    if bid_amount is not None:
        # Ensure bid_amount is sent as a string if it's not null
        params["bid_amount"] = str(bid_amount)
    if tracking_specs is not None: # Add tracking_specs to params if provided
        params["tracking_specs"] = json.dumps(tracking_specs) # Needs to be JSON encoded string
    if creative_id is not None:
        # Creative parameter needs to be a JSON object containing creative_id
        params["creative"] = json.dumps({"creative_id": creative_id})

    if not params:
        return json.dumps({"error": "No update parameters provided (status, bid_amount, tracking_specs, or creative_id)"})

    endpoint = f"{ad_id}"
    try:
        data = await make_api_request(endpoint, access_token, params, method='POST')

        data["ad_id"] = ad_id

        ad_details = await make_api_request(ad_id, access_token, {"fields": "name"})
        if "name" in ad_details:
            data["name"] = ad_details["name"]

        return json.dumps(data)
    except Exception as e:
        return json.dumps({
            "error": "Failed to update ad",
            "details": str(e),
            "params_sent": params
        })


async def _upload_ad_media_core(
    access_token: str,
    account_id: str,
    media_path_url: str,
    name: str,
    media_type: str
) -> tuple[dict, str]:
    if not account_id:
        return {"error": "No account ID provided"}, None

    if not media_path_url:
        return {"error": "No media URL provided"}, None

    if not os.path.exists(media_path_url):
        return {"error": f"Media file not found: {media_path_url}"}, None

    if media_type.upper() not in ["IMAGE", "VIDEO"]:
        media_type = "IMAGE"
    else:
        media_type = media_type.upper()

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    try:
        if not name:
            name = os.path.basename(media_path_url)

        if media_type == "IMAGE":
            with open(media_path_url, "rb") as media_file:
                media_bytes = media_file.read()

            import base64
            encoded_media = base64.b64encode(media_bytes).decode('utf-8')

            endpoint = f"{account_id}/adimages"
            params = {
                "bytes": encoded_media,
                "name": name
            }
            print(f"Uploading image to Facebook Ad Account {account_id}")

            data = await make_api_request(endpoint, access_token, params, method="POST")
            return data, media_type
        else:
            endpoint = f"{account_id}/advideos"
            print(f"Uploading video to Facebook Ad Account {account_id}")

            additional_params = {}
            if name:
                additional_params["title"] = name

            data = await make_api_request_with_file(endpoint, access_token, media_path_url, additional_params)
            return data, media_type

    except Exception as e:
        return {
            "error": f"Failed to upload {media_type.lower()}",
            "details": str(e)
        }, media_type


@mcp_server.tool()
@meta_api_tool
async def upload_ad_media(
    access_token: str = None,
    account_id: str = None,
    media_path_url: str = None,
    name: str = None,
    media_type: str = "IMAGE"
) -> str:
    """
    Upload an image or video to use in Meta Ads creatives.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        media_path_url: URL of the media (image or video) to upload
        name: Optional name for the media (default: derived from URL)
        media_type: Type of media to upload - "IMAGE" or "VIDEO" (default: "IMAGE", invalid values fallback to "IMAGE")

    Returns:
        JSON with hash (for images) or video_id (for videos) for creative creation
    """
    data, resolved_media_type = await _upload_ad_media_core(access_token, account_id, media_path_url, name, media_type)

    if not data or not isinstance(data, dict):
        return json.dumps({"error": "Invalid response from API"})

    if "error" in data:
        return json.dumps(data)

    if resolved_media_type == "IMAGE":
        images = data.get("images")
        if not images or not isinstance(images, dict):
            return json.dumps({"error": "Invalid image upload response", "raw_response": data})
        first_image = next(iter(images.values()), None)
        if not first_image or not isinstance(first_image, dict):
            return json.dumps({"error": "Invalid image data in response", "raw_response": data})
        image_hash = first_image.get("hash")
        if not image_hash:
            return json.dumps({"error": "No hash in image response", "raw_response": data})
        return json.dumps({"hash": image_hash})
    else:
        video_id = data.get("id")
        if not video_id:
            return json.dumps({"error": "No video_id in response", "raw_response": data})
        return json.dumps({"video_id": video_id})


@mcp_server.tool()
@meta_api_tool
async def upload_ad_media_detailed(
    access_token: str = None,
    account_id: str = None,
    media_path_url: str = None,
    name: str = None,
    media_type: str = "IMAGE"
) -> str:
    """
    Upload an image or video to use in Meta Ads creatives. Returns detailed response with URLs.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        media_path_url: URL of the media (image or video) to upload
        name: Optional name for the media (default: derived from URL)
        media_type: Type of media to upload - "IMAGE" or "VIDEO" (default: "IMAGE", invalid values fallback to "IMAGE")

    Returns:
        JSON response with full media details including URLs, dimensions, hash (for images) or video_id (for videos)
    """
    data, resolved_media_type = await _upload_ad_media_core(access_token, account_id, media_path_url, name, media_type)

    if not data or not isinstance(data, dict):
        return json.dumps({"error": "Invalid response from API"})

    if "error" in data:
        return json.dumps(data)

    if resolved_media_type == "VIDEO":
        video_id = data.get("id")
        if not video_id:
            return json.dumps({"error": "No video_id in response", "raw_response": data})
        return json.dumps({
            "success": True,
            "video_id": video_id,
            "details": data
        })

    images = data.get("images")
    if not images or not isinstance(images, dict):
        return json.dumps({"error": "Invalid image upload response", "raw_response": data})

    return json.dumps(data)


_PAC_STORY_POSITIONS = {"facebook": ["story"], "instagram": ["story"]}


def _pac_customization_spec(position_map: dict, publisher_platforms: list) -> dict:
    spec = {"publisher_platforms": list(publisher_platforms)}
    if "facebook" in publisher_platforms and position_map.get("facebook"):
        spec["facebook_positions"] = list(position_map["facebook"])
    if "instagram" in publisher_platforms and position_map.get("instagram"):
        spec["instagram_positions"] = list(position_map["instagram"])
    return spec


@mcp_server.tool()
@meta_api_tool
async def create_ad_creative(
    access_token: str = None,
    account_id: str = None,
    name: str = None,
    image_hash: str = None,
    video_id: str = None,
    page_id: str = None,
    link_url: str = None,
    message: str = None,
    headline: str = None,
    description: str = None,
    call_to_action_type: str = None,
    instagram_actor_id: str = None,
    thumbnail_url: str = None,
    feed_image_hash: str = None,
    story_image_hash: str = None,
    publisher_platforms: list = None
) -> str:
    """
    Create a new ad creative from a single image, a video, or a placement-customized image pair.

    Three mutually exclusive image/video modes:
      1. Single image: pass image_hash (shown, auto-cropped, across all placements).
      2. Video: pass video_id (plus thumbnail_url).
      3. Placement-customized image pair: pass BOTH feed_image_hash and story_image_hash.
         The vertical (9:16) story_image_hash is routed to story placements; the square
         (1:1) feed_image_hash is the default for feed and every other placement, via
         asset_feed_spec / asset_customization_rules — so one creative serves each
         placement correctly-sized and adapts to any placement the ad set targets.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        name: Creative name
        image_hash: Hash of the uploaded image (single-image mode; mutually exclusive with video_id and the feed/story pair)
        video_id: ID of the uploaded video (mutually exclusive with image_hash and the feed/story pair)
        page_id: Facebook Page ID to be used for the ad
        link_url: Destination URL for the ad
        message: Ad copy/text
        headline: Single headline for the ad
        description: Description text
        call_to_action_type: Call to action button type (e.g., 'LEARN_MORE', 'SIGN_UP', 'SHOP_NOW')
        instagram_actor_id: Optional Instagram account ID for Instagram placements
        thumbnail_url: Thumbnail image URL for video creatives (required when video_id is provided)
        feed_image_hash: Square (1:1) image hash used as the default for feed and all non-story placements. Use WITH story_image_hash for placement asset customization; mutually exclusive with image_hash/video_id.
        story_image_hash: Vertical (9:16) image hash routed to story placements. Use WITH feed_image_hash.
        publisher_platforms: Platforms the parent ad set targets, e.g. ["facebook","instagram"], ["facebook"], or ["instagram"]. Instagram placement rules are added only when "instagram" is present; Facebook is always anchored in the creative so Meta can resolve the Page's Instagram identity (a creative scoped to Instagram alone is rejected as "Instagram Account Is Missing"). Defaults to ["facebook","instagram"].

    Returns:
        JSON with creative_id on success
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"})

    use_pac = bool(feed_image_hash) or bool(story_image_hash)

    if use_pac:
        if video_id:
            return json.dumps({"error": "Placement asset customization (feed_image_hash/story_image_hash) cannot be combined with video_id"})
        if image_hash:
            return json.dumps({"error": "Provide either image_hash (single image) OR feed_image_hash+story_image_hash (placement customization), not both"})
        if not (feed_image_hash and story_image_hash):
            return json.dumps({"error": "Placement asset customization requires BOTH feed_image_hash and story_image_hash"})
        # Meta needs >=2 rules over >=2 distinct images; identical hashes -> collapse to single-image path.
        if feed_image_hash == story_image_hash:
            use_pac = False
            image_hash = feed_image_hash
    else:
        if not image_hash and not video_id:
            return json.dumps({"error": "Either image_hash or video_id must be provided"})
        if image_hash and video_id:
            return json.dumps({"error": "Cannot provide both image_hash and video_id - they are mutually exclusive"})

    if not name:
        name = f"Creative {int(time.time())}"

    # Ensure account_id has the 'act_' prefix
    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    creative_data = {
        "name": name
    }

    if use_pac:
        platforms = publisher_platforms or ["facebook", "instagram"]
        # Always anchor Facebook in the creative's customization rules, even when the ad set is
        # Instagram-only. The Facebook Page is the identity Meta uses to auto-resolve the Page's
        # page-backed Instagram account; a creative scoped to Instagram alone has no such anchor,
        # so Meta rejects the ad with "Instagram Account Is Missing". The ad set's own
        # publisher_platforms still governs delivery, so the Facebook anchor stays inert on an
        # Instagram-only ad set.
        creative_platforms = platforms if "facebook" in platforms else ["facebook", *platforms]
        object_story_spec = {"page_id": page_id}
        if instagram_actor_id:
            object_story_spec["instagram_user_id"] = instagram_actor_id
        creative_data["object_story_spec"] = object_story_spec

        asset_feed_spec = {
            "images": [
                {"hash": feed_image_hash, "adlabels": [{"name": "feed_img"}]},
                {"hash": story_image_hash, "adlabels": [{"name": "story_img"}]},
            ],
            "ad_formats": ["SINGLE_IMAGE"],
            "asset_customization_rules": [
                # Vertical (9:16) image for story-canvas placements.
                {
                    "image_label": {"name": "story_img"},
                    "customization_spec": _pac_customization_spec(_PAC_STORY_POSITIONS, creative_platforms),
                },
                # Square (1:1) image as the catch-all for every placement not matched by the
                # story rule above. Placement customization uses an empty customization_spec as
                # the catch-all; is_default is a Multi-Language-ads-only field and must not be set
                # here (setting it makes Meta drop the fallback and the ad becomes uncoverable).
                {
                    "image_label": {"name": "feed_img"},
                    "customization_spec": {},
                },
            ],
        }
        if message:
            asset_feed_spec["bodies"] = [{"text": message}]
        if headline:
            asset_feed_spec["titles"] = [{"text": headline}]
        if description:
            asset_feed_spec["descriptions"] = [{"text": description}]
        if link_url:
            asset_feed_spec["link_urls"] = [{"website_url": link_url}]
        if call_to_action_type:
            asset_feed_spec["call_to_action_types"] = [call_to_action_type]

        creative_data["asset_feed_spec"] = asset_feed_spec
    elif video_id:
        video_data = {
            "video_id": video_id
        }

        if thumbnail_url:
            video_data["image_url"] = thumbnail_url

        if message:
            video_data["message"] = message

        if headline:
            video_data["title"] = headline

        if description:
            video_data["link_description"] = description

        if call_to_action_type:
            call_to_action_data = {
                "type": call_to_action_type
            }
            if link_url:
                call_to_action_data["value"] = {
                    "link": link_url
                }
            video_data["call_to_action"] = call_to_action_data

        creative_data["object_story_spec"] = {
            "page_id": page_id,
            "video_data": video_data
        }
    else:
        creative_data["object_story_spec"] = {
            "page_id": page_id,
            "link_data": {
                "image_hash": image_hash,
                "link": link_url if link_url else "https://facebook.com"
            }
        }

        if message:
            creative_data["object_story_spec"]["link_data"]["message"] = message

        if headline:
            creative_data["object_story_spec"]["link_data"]["name"] = headline

        if description:
            creative_data["object_story_spec"]["link_data"]["description"] = description

        if call_to_action_type:
            creative_data["object_story_spec"]["link_data"]["call_to_action"] = {
                "type": call_to_action_type
            }
    
    if instagram_actor_id and not use_pac:
        creative_data["instagram_actor_id"] = instagram_actor_id

    # Prepare the API endpoint for creating a creative
    endpoint = f"{account_id}/adcreatives"
    
    try:
        # Make API request to create the creative
        data = await make_api_request(endpoint, access_token, creative_data, method="POST")
        
        if "id" in data:
            return json.dumps({
                "success": True,
                "creative_id": data["id"]
            })
        
        return json.dumps(data)
    
    except Exception as e:
        return json.dumps({
            "error": "Failed to create ad creative",
            "details": str(e),
            "creative_data_sent": creative_data
        })


@mcp_server.tool()
@meta_api_tool
async def update_ad_creative(
    access_token: str = None,
    creative_id: str = None,
    name: str = None,
    status: str = None,
    account_id: str = None
) -> str:
    """
    Update an ad creative's metadata (name, status, or account).

    IMPORTANT: Ad creative content (message, headline, description, images, videos) is IMMUTABLE.
    According to Meta API documentation, only the following fields can be updated:
    - name: Creative name in the library
    - status: Creative status (ACTIVE, PAUSED, DELETED, etc.)
    - account_id: Ad account ID

    To "update" ad content (message, headline, description):
    1. Create a NEW creative with the updated content using create_ad_creative()
    2. Update the AD (not the creative) to reference the new creative using update_ad(creative_id=new_creative_id)

    This is how Meta's system works - creatives are immutable content objects, and ads reference them.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        creative_id: Meta Ads creative ID to update
        name: Creative name (up to 100 characters)
        status: Creative status (ACTIVE, IN_PROCESS, WITH_ISSUES, DELETED)
        account_id: Ad account ID (format: act_XXXXXXXXX)

    Returns:
        JSON response with updated creative details
    """
    if not creative_id:
        return json.dumps({"error": "No creative ID provided"})

    update_data = {}

    if name:
        update_data["name"] = name

    if status:
        update_data["status"] = status

    if account_id:
        update_data["account_id"] = account_id

    if not update_data:
        return json.dumps({
            "error": "No update parameters provided",
            "note": "Only name, status, and account_id can be updated. To change content (message, headline, description), create a new creative and update the ad to use it."
        })

    endpoint = f"{creative_id}"

    try:
        data = await make_api_request(endpoint, access_token, update_data, method="POST")

        if "id" in data or "success" in data:
            creative_endpoint = f"{creative_id}"
            creative_params = {
                "fields": "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,url_tags,link_url"
            }

            creative_details = await make_api_request(creative_endpoint, access_token, creative_params)

            if "id" not in creative_details:
                creative_details["id"] = creative_id

            return json.dumps({
                "success": True,
                "creative_id": creative_id,
                "details": creative_details
            })

        return json.dumps(data)

    except Exception as e:
        return json.dumps({
            "error": "Failed to update ad creative",
            "details": str(e),
            "update_data_sent": update_data
        })


async def _discover_pages_for_account(account_id: str, access_token: str) -> dict:
    """
    Internal function to discover pages for an account using multiple approaches.
    Returns the best available page ID for ad creation.
    """
    try:
        # Approach 1: Extract page IDs from tracking_specs in ads (most reliable)
        endpoint = f"{account_id}/ads"
        params = {
            "fields": "id,name,adset_id,campaign_id,status,creative,created_time,updated_time,bid_amount,conversion_domain,tracking_specs",
            "limit": 100
        }
        
        tracking_ads_data = await make_api_request(endpoint, access_token, params)
        
        tracking_page_ids = set()
        if "data" in tracking_ads_data:
            for ad in tracking_ads_data.get("data", []):
                tracking_specs = ad.get("tracking_specs", [])
                if isinstance(tracking_specs, list):
                    for spec in tracking_specs:
                        if isinstance(spec, dict) and "page" in spec:
                            page_list = spec["page"]
                            if isinstance(page_list, list):
                                for page_id in page_list:
                                    if isinstance(page_id, (str, int)) and str(page_id).isdigit():
                                        tracking_page_ids.add(str(page_id))
        
        if tracking_page_ids:
            # Get details for the first page found
            page_id = list(tracking_page_ids)[0]
            page_endpoint = f"{page_id}"
            page_params = {
                "fields": "id,name,username,category,fan_count,link,verification_status,picture"
            }
            
            page_data = await make_api_request(page_endpoint, access_token, page_params)
            if "id" in page_data:
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_name": page_data.get("name", "Unknown"),
                    "source": "tracking_specs",
                    "note": "Page ID extracted from existing ads - most reliable for ad creation"
                }
        
        # Approach 2: Try client_pages endpoint
        endpoint = f"{account_id}/client_pages"
        params = {
            "fields": "id,name,username,category,fan_count,link,verification_status,picture"
        }
        
        client_pages_data = await make_api_request(endpoint, access_token, params)
        
        if "data" in client_pages_data and client_pages_data["data"]:
            page = client_pages_data["data"][0]
            return {
                "success": True,
                "page_id": page["id"],
                "page_name": page.get("name", "Unknown"),
                "source": "client_pages"
            }
        
        # Approach 3: Try assigned_pages endpoint
        pages_endpoint = f"{account_id}/assigned_pages"
        pages_params = {
            "fields": "id,name",
            "limit": 1 
        }
        
        pages_data = await make_api_request(pages_endpoint, access_token, pages_params)
        
        if "data" in pages_data and pages_data["data"]:
            page = pages_data["data"][0]
            return {
                "success": True,
                "page_id": page["id"],
                "page_name": page.get("name", "Unknown"),
                "source": "assigned_pages"
            }
        
        # If all approaches failed
        return {
            "success": False,
            "message": "No suitable pages found for this account",
            "note": "Try using get_account_pages to see all available pages or provide page_id manually"
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Error during page discovery: {str(e)}"
        }


async def _search_pages_by_name_core(access_token: str, account_id: str, search_term: str = None) -> str:
    """
    Core logic for searching pages by name.
    
    Args:
        access_token: Meta API access token
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        search_term: Search term to find pages by name (optional - returns all pages if not provided)
    
    Returns:
        JSON string with search results
    """
    # Ensure account_id has the 'act_' prefix
    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"
    
    try:
        # Use the internal discovery function directly
        page_discovery_result = await _discover_pages_for_account(account_id, access_token)
        
        if not page_discovery_result.get("success"):
            return json.dumps({
                "data": [],
                "message": "No pages found for this account",
                "details": page_discovery_result.get("message", "Page discovery failed")
            })
        
        # Create a single page result
        page_data = {
            "id": page_discovery_result["page_id"],
            "name": page_discovery_result.get("page_name", "Unknown"),
            "source": page_discovery_result.get("source", "unknown")
        }
        
        all_pages_data = {"data": [page_data]}
        
        # Filter pages by search term if provided
        if search_term:
            search_term_lower = search_term.lower()
            filtered_pages = []
            
            for page in all_pages_data["data"]:
                page_name = page.get("name", "").lower()
                if search_term_lower in page_name:
                    filtered_pages.append(page)
            
            return json.dumps({
                "data": filtered_pages,
                "search_term": search_term,
                "total_found": len(filtered_pages),
                "total_available": len(all_pages_data["data"])
            })
        else:
            # Return all pages if no search term provided
            return json.dumps({
                "data": all_pages_data["data"],
                "total_available": len(all_pages_data["data"]),
                "note": "Use search_term parameter to filter pages by name"
            })
    
    except Exception as e:
        return json.dumps({
            "error": "Failed to search pages by name",
            "details": str(e)
        })


@mcp_server.tool()
@meta_api_tool
async def search_pages_by_name(access_token: str = None, account_id: str = None, search_term: str = None) -> str:
    """
    Search for pages by name within an account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        search_term: Search term to find pages by name (optional - returns all pages if not provided)
    
    Returns:
        JSON response with matching pages
    """
    # Check required parameters
    if not account_id:
        return json.dumps({"error": "No account ID provided"})
    
    # Call the core function
    result = await _search_pages_by_name_core(access_token, account_id, search_term)
    return result


@mcp_server.tool()
@meta_api_tool
async def get_account_pages(access_token: str = None) -> str:
    """
    Get pages associated with all businesses accessible to the user.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
    
    Returns:
        JSON response with pages from all businesses
    """
    import asyncio
    
    try:
        # Step 1: Get all businesses for the user
        businesses_endpoint = "me/businesses"
        businesses_params = {
            "limit": 1000
        }
        
        businesses_data = await make_api_request(businesses_endpoint, access_token, businesses_params)
        
        if "error" in businesses_data:
            return json.dumps({
                "error": "Failed to get businesses",
                "details": businesses_data.get("error")
            })
        
        if "data" not in businesses_data or not businesses_data["data"]:
            return json.dumps({
                "data": [],
                "message": "No businesses found for this user"
            })
        
        businesses = businesses_data["data"]
        
        # Step 2: Get owned pages for each business in parallel
        async def get_business_pages(business):
            business_id = business["id"]
            business_name = business.get("name", "Unknown")
            
            try:
                pages_endpoint = f"{business_id}/owned_pages"
                pages_params = {
                    "fields": "id,name,fan_count",
                    "limit": 1000
                }
                
                pages_data = await make_api_request(pages_endpoint, access_token, pages_params)
                
                if "error" in pages_data:
                    raise Exception(f"Business '{business_name}' (ID: {business_id}): {pages_data['error']}")
                
                return pages_data.get("data", [])
                
            except Exception as e:
                # Re-raise the exception to ensure it's propagated and not silently swallowed
                raise Exception(f"Failed to get pages for business '{business_name}' (ID: {business_id}): {str(e)}")
        
        # Execute business page requests sequentially for safety
        all_pages = []
        seen_page_names = set()
        
        for business in businesses:
            try:
                business_pages = await get_business_pages(business)
                
                # Add pages to results and deduplicate by name
                for page in business_pages:
                    page_name = page.get("name")
                    if page_name and page_name not in seen_page_names:
                        seen_page_names.add(page_name)
                        all_pages.append(page)
                        
            except Exception as e:
                return json.dumps({
                    "error": "Failed to get pages from business",
                    "details": str(e)
                })
        
        # Sort by fan_count descending (highest first)
        all_pages.sort(key=lambda page: page.get("fan_count", 0), reverse=True)
        
        # Step 3: Validate page access using batch API
        if not all_pages:
            return json.dumps({
                "data": [],
                "total_pages_with_access": 0,
                "total_pages_discovered": 0
            })
        
        # Prepare batch requests to validate page access
        batch_requests = []
        for page in all_pages:
            page_id = page.get("id")
            if page_id:
                batch_requests.append({
                    "method": "GET",
                    "relative_url": f"{page_id}?fields=id,name"
                })
        
        # Make batch request to validate access
        batch_responses = await make_batch_api_request(batch_requests, access_token)
        
        # Filter pages based on successful batch responses
        accessible_pages = []
        for i, response in enumerate(batch_responses):
            if response.get("code") == 200:
                # Parse the successful response body
                try:
                    response_body = json.loads(response.get("body", "{}"))
                    if "id" in response_body and "name" in response_body:
                        accessible_pages.append({
                            "id": response_body["id"],
                            "name": response_body["name"]
                        })
                except json.JSONDecodeError:
                    # Skip pages with invalid response bodies
                    continue
            # Ignore pages with error responses (code != 200)
        
        return json.dumps({
            "data": accessible_pages,
            "total_pages_with_access": len(accessible_pages),
            "total_pages_discovered": len(all_pages),
            "note": "Only pages with verified access are included"
        })
        
    except Exception as e:
        return json.dumps({
            "error": "Failed to get account pages",
            "details": str(e)
        })


@mcp_server.tool()
@meta_api_tool
async def get_account_pixels(access_token: str = None, account_id: str = None) -> str:
    """
    Get Meta pixels associated with an ads account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
    
    Returns:
        JSON response with pixels associated with the account
    """
    if not account_id:
        return json.dumps({"error": "No account ID provided"})
    
    # Ensure account_id has the 'act_' prefix
    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"
    
    try:
        endpoint = f"{account_id}/adspixels"
        params = {
            "fields": "id,name,last_fired_time"
        }
        
        data = await make_api_request(endpoint, access_token, params)
        
        return json.dumps(data)
        
    except Exception as e:
        return json.dumps({
            "error": "Failed to get account pixels",
            "details": str(e)
        })
