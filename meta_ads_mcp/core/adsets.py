"""Ad Set-related functionality for Meta Ads API."""

import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from .api import meta_api_tool, make_api_request
from .accounts import get_ad_accounts
from .server import mcp_server
from .adset_enums import AdSetStatus, OptimizationGoal, BillingEvent, BidStrategy, DestinationType


@mcp_server.tool()
@meta_api_tool
async def get_adsets(access_token: str = None, account_id: str = None, limit: int = 10, campaign_id: str = "") -> str:
    """
    Get ad sets for a Meta Ads account with optional filtering by campaign.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of ad sets to return (default: 10)
        campaign_id: Optional campaign ID to filter by
    """
    if not account_id:
        return json.dumps({"error": "No account ID specified"})
    
    # Change endpoint based on whether campaign_id is provided
    if campaign_id:
        endpoint = f"{campaign_id}/adsets"
        params = {
            "fields": "id,name,campaign_id,status,daily_budget,lifetime_budget,targeting,bid_amount,bid_strategy,optimization_goal,billing_event,start_time,end_time,created_time,updated_time,frequency_control_specs{event,interval_days,max_frequency},promoted_object,learning_stage_info",
            "limit": limit
        }
    else:
        # Use account endpoint if no campaign_id is given
        endpoint = f"{account_id}/adsets"
        params = {
            "fields": "id,name,campaign_id,status,daily_budget,lifetime_budget,targeting,bid_amount,bid_strategy,optimization_goal,billing_event,start_time,end_time,created_time,updated_time,frequency_control_specs{event,interval_days,max_frequency},promoted_object,learning_stage_info",
            "limit": limit
        }
        # Note: Removed the attempt to add campaign_id to params for the account endpoint case, 
        # as it was ineffective and the logic now uses the correct endpoint for campaign filtering.

    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def get_adset_details(access_token: str = None, adset_id: str = None) -> str:
    """
    Get detailed information about a specific ad set.
    
    Args:
        adset_id: Meta Ads ad set ID (required)
        access_token: Meta API access token (optional - will use cached token if not provided)
    
    Example:
        To call this function through MCP, pass the adset_id as the first argument:
        {
            "args": "YOUR_ADSET_ID"
        }
    """
    if not adset_id:
        return json.dumps({"error": "No ad set ID provided"})
    
    endpoint = f"{adset_id}"
    # Explicitly prioritize frequency_control_specs in the fields request
    params = {
        "fields": "id,name,campaign_id,status,frequency_control_specs{event,interval_days,max_frequency},daily_budget,lifetime_budget,targeting,bid_amount,bid_strategy,optimization_goal,billing_event,start_time,end_time,created_time,updated_time,attribution_spec,destination_type,promoted_object,pacing_type,budget_remaining,dsa_beneficiary"
    }
    
    data = await make_api_request(endpoint, access_token, params)
    
    # For debugging - check if frequency_control_specs was returned
    if 'frequency_control_specs' not in data:
        data['_meta'] = {
            'note': 'No frequency_control_specs field was returned by the API. This means either no frequency caps are set or the API did not include this field in the response.'
        }
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def create_adset(
    account_id: str = None,
    campaign_id: str = None,
    name: str = None,
    status: AdSetStatus = AdSetStatus.PAUSED,
    targeting: Dict[str, Any] = None,
    optimization_goal: Optional[OptimizationGoal] = None,
    billing_event: Optional[BillingEvent] = None,
    bid_amount = None,
    bid_strategy: Optional[BidStrategy] = None,
    start_time: str = None,
    end_time: str = None,
    dsa_beneficiary: str = None,
    dsa_payor: str = None,
    promoted_object: Dict[str, Any] = None,
    destination_type: Optional[DestinationType] = None,
    attribution_spec: List[Dict[str, Any]] = None,
    access_token: str = None
) -> str:
    """
    Create a new ad set in a Meta Ads account.
    
    Args:
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        campaign_id: Meta Ads campaign ID this ad set belongs to
        name: Ad set name
        status: Initial ad set status (default: PAUSED)
        Note: Budgets are managed at the campaign level only. This tool does not accept ad set budgets.
        targeting: Pass 'targeting' as a complete dictionary object containing all targeting specifications.
                  Do not pass individual targeting fields as separate parameters.
                  Use targeting_automation.advantage_audience=1 for automatic audience finding.
                  Note: Advanced targeting features are NOT supported.
                  Example format (use your own values): {"age_max": 65, "age_min": 18, "genders": [1, 2], "geo_locations": {"cities": [{"country": "NL", "distance_unit": "mile", "key": "1648467", "name": "Arnhem", "radius": 11, "region": "Gelderland", "region_id": "2662"}], "location_types": ["home", "recent"]}, "locales": [14], "targeting_relaxation_types": {"lookalike": 0, "custom_audience": 0}, "publisher_platforms": ["facebook", "instagram"], "facebook_positions": ["feed", "groups_feed", "profile_feed", "story"], "instagram_positions": ["stream", "story"], "device_platforms": ["mobile", "desktop"]}
                  Key targeting parameters to include in the single object:
                  - age_min: Minimum age (13-65, defaults to 18)
                  - age_max: Maximum age (13-65, must be 65 or lower)
                  - genders: Array targeting specific genders (1=males, 2=females, omit for all)
                  - locales: Array of language locale IDs (numeric). Target users with specific languages
                  - geo_locations.cities: Array of city targeting objects with required fields:
                    * key: City identifier from Meta's location database
                    * radius: Distance around city (10-50 miles or 17-80 kilometers)
                    * distance_unit: "mile" or "kilometer"
                    * Limit: 250 cities maximum
                  - geo_locations.location_types: Must be ["home", "recent"] if specified
                  - publisher_platforms: Platforms to show ads on. Can specify facebook only, instagram only, or both (e.g., ["facebook"], ["instagram"], or ["facebook", "instagram"]). Also supports audience_network and messenger.
                  - facebook_positions: Facebook ad placement positions. Only needed when facebook is in publisher_platforms. Select any of the allowed positions: feed, right_hand_column, marketplace, video_feeds, story, search, instream_video, facebook_reels, facebook_reels_overlay, profile_feed, notification.
                  - instagram_positions: Instagram ad placement positions. Only needed when instagram is in publisher_platforms. Select any of the allowed positions: stream (main Instagram feed), story, explore, explore_home, reels, profile_feed, ig_search, profile_reels. Optional, defaults to all positions if not specified.
                  - device_platforms: Target devices (mobile, desktop)
        optimization_goal: Conversion optimization goal
        billing_event: How you're charged
        bid_amount: Bid amount in account currency (in cents)
        bid_strategy: Bid strategy. If you enable campaign budget optimization, you should set bid_strategy at the parent campaign level.
                     If you do not enable campaign budget optimization, you should set bid_strategy at the ad set level.
        start_time: Start time in ISO 8601 format (e.g., '2023-12-01T12:00:00-0800'). Defaults to today if not provided.
        end_time: End time in ISO 8601 format
        dsa_beneficiary: DSA beneficiary (person/organization benefiting from ads) for European compliance
        dsa_payor: DSA payor (person/organization paying for ads) for European compliance
        promoted_object: Pass 'promoted_object' as a complete dictionary object containing promotion configuration.
                        Do not pass application_id, pixel_id, etc. as separate parameters.
                        Example format (use your own values):
                        For mobile apps: {"application_id": "123456789012345", "object_store_url": "https://apps.apple.com/app/id123456789"}
                        For conversion tracking: {"pixel_id": "123456789012345", "custom_event_type": "LEAD"}
                        For page promotion: {"page_id": "123456789012345"}
                        Object requirements:
                        - For apps: application_id + object_store_url
                        - For conversions: pixel_id + custom_event_type (PURCHASE, LEAD, COMPLETE_REGISTRATION, ADD_TO_CART, etc.)
                        - For pages: page_id
                        - pixel_id: Facebook conversion pixel ID (numeric string) for offsite conversions
                        - Must have permissions for promoted objects
        destination_type: Where users are directed after clicking the ad
        attribution_spec: Pass 'attribution_spec' as a list of dictionary objects for conversion tracking.
                         Do not pass event_type, window_days, etc. as separate parameters.
                         Example format (use your own values): [{"event_type": "CLICK_THROUGH", "window_days": 7}, {"event_type": "VIEW_THROUGH", "window_days": 1}]
                         Each dictionary object must contain:
                         - event_type: Attribution event type (CLICK_THROUGH, VIEW_THROUGH)
                         - window_days: Attribution window in days (1-28)
        access_token: Meta API access token (optional - will use cached token if not provided)
    """
    # Check required parameters
    if not account_id:
        return json.dumps({"error": "No account ID provided"})
    
    if not campaign_id:
        return json.dumps({"error": "No campaign ID provided"})
    
    if not name:
        return json.dumps({"error": "No ad set name provided"})
    
    if not optimization_goal:
        return json.dumps({"error": "No optimization goal provided"})
    
    if not billing_event:
        return json.dumps({"error": "No billing event provided"})
    
    endpoint = f"{account_id}/adsets"
    
    params = {
        "name": name,
        "campaign_id": campaign_id,
        "status": getattr(status, 'value', status),
        "optimization_goal": getattr(optimization_goal, 'value', optimization_goal),
        "billing_event": getattr(billing_event, 'value', billing_event),
        "targeting": json.dumps(targeting)
    }
    
    # Add other parameters if provided
    if bid_amount is not None:
        params["bid_amount"] = str(bid_amount)
    
    if bid_strategy is not None:
        params["bid_strategy"] = getattr(bid_strategy, 'value', bid_strategy)
    
    if not start_time:
        start_time = datetime.utcnow().strftime('%Y-%m-%dT00:00:00+0000')
    params["start_time"] = start_time
    
    if end_time:
        params["end_time"] = end_time
    
    # Add DSA beneficiary if provided
    if dsa_beneficiary:
        params["dsa_beneficiary"] = dsa_beneficiary
    
    # Add DSA payor if provided
    if dsa_payor:
        params["dsa_payor"] = dsa_payor
    
    # Add mobile app parameters if provided
    if promoted_object:
        params["promoted_object"] = json.dumps(promoted_object)
    
    if destination_type is not None:
        params["destination_type"] = getattr(destination_type, 'value', destination_type)
    
    # Add attribution spec if provided
    if attribution_spec:
        params["attribution_spec"] = json.dumps(attribution_spec)

    try:
        data = await make_api_request(endpoint, access_token, params, method="POST")
        return json.dumps(data)
    except Exception as e:
        return json.dumps({
            "error": "Failed to create ad set",
            "details": str(e),
            "params_sent": params
        })


@mcp_server.tool()
@meta_api_tool
async def update_adset(adset_id: str, frequency_control_specs: List[Dict[str, Any]] = None, bid_strategy: Optional[BidStrategy] = None,
                        bid_amount: int = None, status: Optional[AdSetStatus] = None, targeting: Dict[str, Any] = None,
                        optimization_goal: Optional[OptimizationGoal] = None, daily_budget = None, lifetime_budget = None,
                        start_time: str = None, end_time: str = None,
                        attribution_spec: List[Dict[str, Any]] = None,
                        access_token: str = None) -> str:
    """
    Update an ad set with new settings including frequency caps and budgets.
    
    Args:
        adset_id: Meta Ads ad set ID
        frequency_control_specs: List of frequency control specifications
                                 (e.g. [{"event": "IMPRESSIONS", "interval_days": 7, "max_frequency": 3}])
        bid_strategy: Bid strategy. If you enable campaign budget optimization, you should set bid_strategy at the parent campaign level.
                     If you do not enable campaign budget optimization, you should set bid_strategy at the ad set level.
        bid_amount: Bid amount in account currency (in cents)
        status: Update ad set status
        targeting: Pass 'targeting' as a complete dictionary object containing all targeting specifications.
                  Do not pass individual targeting fields as separate parameters.
                  Use targeting_automation.advantage_audience=1 for automatic audience finding.
                  Note: Advanced targeting features are NOT supported. This will REPLACE existing targeting.
                  Example format (use your own values): {"age_max": 65, "age_min": 18, "genders": [1, 2], "geo_locations": {"cities": [{"country": "NL", "distance_unit": "mile", "key": "1648467", "name": "Arnhem", "radius": 11, "region": "Gelderland", "region_id": "2662"}], "location_types": ["home", "recent"]}, "locales": [14], "targeting_relaxation_types": {"lookalike": 0, "custom_audience": 0}, "publisher_platforms": ["facebook", "instagram"], "facebook_positions": ["feed", "groups_feed", "profile_feed", "story"], "instagram_positions": ["stream", "story"], "device_platforms": ["mobile", "desktop"]}
                  Key targeting parameters to include in the single object:
                  - age_min: Minimum age (13-65, defaults to 18)
                  - age_max: Maximum age (13-65, must be 65 or lower)
                  - genders: Array targeting specific genders (1=males, 2=females, omit for all)
                  - locales: Array of language locale IDs (numeric). Target users with specific languages
                  - geo_locations.cities: Array of city targeting objects with required fields:
                    * key: City identifier from Meta's location database
                    * radius: Distance around city (10-50 miles or 17-80 kilometers)
                    * distance_unit: "mile" or "kilometer"
                    * Limit: 250 cities maximum
                  - geo_locations.location_types: Must be ["home", "recent"] if specified
                  - publisher_platforms: Platforms to show ads on. Can specify facebook only, instagram only, or both (e.g., ["facebook"], ["instagram"], or ["facebook", "instagram"]). Also supports audience_network and messenger.
                  - facebook_positions: Facebook ad placement positions. Only needed when facebook is in publisher_platforms. Select any of the allowed positions: feed, right_hand_column, marketplace, video_feeds, story, search, instream_video, facebook_reels, facebook_reels_overlay, profile_feed, notification.
                  - instagram_positions: Instagram ad placement positions. Only needed when instagram is in publisher_platforms. Select any of the allowed positions: stream (main Instagram feed), story, explore, explore_home, reels, profile_feed, ig_search, profile_reels. Optional, defaults to all positions if not specified.
                  - device_platforms: Target devices (mobile, desktop)
        optimization_goal: Conversion optimization goal
        daily_budget: Daily budget in account currency (in cents) as a string
        lifetime_budget: Lifetime budget in account currency (in cents) as a string
        start_time: Start time in ISO 8601 format (e.g., '2023-12-01T12:00:00-0800'). ONLY editable before delivery begins.
        end_time: End time in ISO 8601 format. Editable anytime, must be in future.
        attribution_spec: Pass 'attribution_spec' as a list of dictionary objects for conversion tracking.
                         Do not pass event_type, window_days, etc. as separate parameters.
                         Example format (use your own values): [{"event_type": "CLICK_THROUGH", "window_days": 7}, {"event_type": "VIEW_THROUGH", "window_days": 1}]
                         Each dictionary object must contain:
                         - event_type: Attribution event type (CLICK_THROUGH, VIEW_THROUGH)
                         - window_days: Attribution window in days (1-28)
        access_token: Meta API access token (optional - will use cached token if not provided)
    """
    if not adset_id:
        return json.dumps({"error": "No ad set ID provided"})
    
    params = {}
    
    if frequency_control_specs is not None:
        params['frequency_control_specs'] = frequency_control_specs
    
    if bid_strategy is not None:
        params['bid_strategy'] = getattr(bid_strategy, 'value', bid_strategy)

    if bid_amount is not None:
        params['bid_amount'] = str(bid_amount)

    if status is not None:
        params['status'] = getattr(status, 'value', status)

    if optimization_goal is not None:
        params['optimization_goal'] = getattr(optimization_goal, 'value', optimization_goal)
        
    if targeting is not None:
        # Ensure proper JSON encoding for targeting
        if isinstance(targeting, dict):
            params['targeting'] = json.dumps(targeting)
        else:
            params['targeting'] = targeting  # Already a string
    
    # Add budget parameters if provided
    if daily_budget is not None:
        params['daily_budget'] = str(daily_budget)

    if lifetime_budget is not None:
        params['lifetime_budget'] = str(lifetime_budget)

    # Add scheduling parameters if provided
    if start_time is not None:
        params['start_time'] = start_time
    if end_time is not None:
        params['end_time'] = end_time

    # Add attribution spec if provided
    if attribution_spec is not None:
        params['attribution_spec'] = json.dumps(attribution_spec)

    if not params:
        return json.dumps({"error": "No update parameters provided"})

    endpoint = f"{adset_id}"
    
    try:
        # Use POST method for updates as per Meta API documentation
        data = await make_api_request(endpoint, access_token, params, method="POST")

        # Ensure adset_id is included in the response
        data["adset_id"] = adset_id

        # Fetch adset details to ensure name is included in response
        adset_details = await make_api_request(adset_id, access_token, {"fields": "name"})
        if "name" in adset_details:
            data["name"] = adset_details["name"]

        return json.dumps(data)
    except Exception as e:
        error_msg = str(e)
        # Include adset_id in error for better context
        return json.dumps({
            "error": f"Failed to update ad set {adset_id}",
            "details": error_msg,
            "params_sent": params
        }) 
