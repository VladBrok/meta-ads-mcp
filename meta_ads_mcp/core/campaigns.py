"""Campaign-related functionality for Meta Ads API."""

import json
from typing import List, Optional, Dict, Any, Union
from .api import meta_api_tool, make_api_request, make_batch_api_request
from .accounts import get_ad_accounts
from .server import mcp_server
from .campaign_enums import CampaignObjective, CampaignStatus, CampaignBidStrategy


@mcp_server.tool()
@meta_api_tool
async def get_campaigns(access_token: str = None, account_id: str = None, limit: int = 10, status_filter: str = "", after: str = "", campaign_ids: Optional[List[str]] = None) -> str:
    """
    Get campaigns for a Meta Ads account with optional filtering.
    
    Note: By default, the Meta API returns a subset of available fields. 
    Other fields like 'effective_status', 'special_ad_categories', 'promoted_object', etc., might be available 
    but require specifying them in the API call (currently not exposed by this tool's parameters).
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        limit: Maximum number of campaigns to return (default: 10)
        status_filter: Filter by effective status (e.g., 'ACTIVE', 'PAUSED', 'ARCHIVED').
                       Maps to the 'effective_status' API parameter, which expects an array
                       (this function handles the required JSON formatting). Leave empty for all statuses.
        after: Pagination cursor to get the next set of results
        campaign_ids: internal field, do not use
    """
    if not account_id:
        return json.dumps({"error": "No account ID specified"})
    
    endpoint = f"{account_id}/campaigns"
    params = {
        "fields": "id,name,objective,status,daily_budget,lifetime_budget,buying_type,start_time,stop_time,created_time,updated_time,bid_strategy,source_campaign_id,budget_remaining,spend_cap",
        "limit": limit
    }
    
    if status_filter:
        # API expects an array, encode it as a JSON string
        params["effective_status"] = json.dumps([status_filter])
    
    if after:
        params["after"] = after
    
    if campaign_ids is not None:
        filtering = [{"field": "campaign.id", "operator": "IN", "value": campaign_ids}]
        params["filtering"] = json.dumps(filtering)
    
    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def get_campaign_details(access_token: str = None, account_id: str = None, campaign_id: str = None, name_contains: str = None) -> str:
    """
    Get detailed information about a specific campaign by ID or search campaigns by name.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (required, format: act_XXXXXXXXX)
        campaign_id: Meta Ads campaign ID (alternative to name_contains)
        name_contains: Search for campaigns containing this text in their name (alternative to campaign_id)
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"})
    
    if name_contains and name_contains.strip():
        endpoint = f"{account_id}/campaigns"
        filtering = [{"field": "name", "operator": "CONTAIN", "value": name_contains.strip()}]
        params = {
            "fields": "id,name,objective,status,daily_budget,lifetime_budget,buying_type,start_time,stop_time,created_time,updated_time,bid_strategy,special_ad_categories,special_ad_category_country,budget_remaining,configured_status,source_campaign_id,spend_cap",
            "filtering": json.dumps(filtering)
        }
    else:
        if not campaign_id:
            return json.dumps({"error": "Either campaign_id or name_contains must be provided"})
        
        endpoint = f"{campaign_id}"
        params = {
            "fields": "id,name,objective,status,daily_budget,lifetime_budget,buying_type,start_time,stop_time,created_time,updated_time,bid_strategy,special_ad_categories,special_ad_category_country,budget_remaining,configured_status,source_campaign_id,spend_cap"
        }
    
    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def create_campaign(
    access_token: str = None,
    account_id: str = None,
    name: str = None,
    objective: Optional[CampaignObjective] = None,
    special_ad_categories: List[str] = None,
    special_ad_category_country: str = None,
    daily_budget = None,
    lifetime_budget = None,
    buying_type: str = None,
    bid_strategy: Optional[CampaignBidStrategy] = None,
    bid_cap = None,
    spend_cap = None,
    campaign_budget_optimization: bool = None
) -> str:
    """
    Create a new campaign in a Meta Ads account. Campaign is created as paused by default.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (format: act_XXXXXXXXX)
        name: Campaign name
        objective: Campaign objective (outcome-based)
        special_ad_categories: List of special ad categories if applicable
        special_ad_category_country: Country for special ad categories (e.g., 'NL', 'DE', 'US')
        daily_budget: Daily budget in account currency (in cents) as a string. Budgets are managed at the campaign level.
        lifetime_budget: Lifetime budget in account currency (in cents) as a string. Budgets are managed at the campaign level.
        buying_type: Buying type (e.g., 'AUCTION')
        bid_strategy: Bid strategy. If you enable campaign budget optimization, you should set bid_strategy at the parent campaign level.
                     If you do not enable campaign budget optimization, you should set bid_strategy at the ad set level.
        bid_cap: Bid cap in account currency (in cents) as a string
        spend_cap: Spending limit for the campaign in account currency (in cents) as a string, should be at least 10000 cents
        campaign_budget_optimization: Whether to enable campaign budget optimization.
    """
    # Check required parameters
    if not account_id:
        return json.dumps({"error": "No account ID provided"})
    
    if not name:
        return json.dumps({"error": "No campaign name provided"})
        
    if not objective:
        return json.dumps({"error": "No campaign objective provided"})
    
    # Special_ad_categories is required by the API, set default if not provided
    if special_ad_categories is None:
        special_ad_categories = []
    
    endpoint = f"{account_id}/campaigns"
    
    params = {
        "name": name,
        "objective": getattr(objective, 'value', objective),
        "status": "PAUSED",
        "special_ad_categories": json.dumps(special_ad_categories)
    }
    
    # Always use campaign-level budgets if provided
    if daily_budget is not None:
        params["daily_budget"] = str(daily_budget)
    if lifetime_budget is not None:
        params["lifetime_budget"] = str(lifetime_budget)
    if campaign_budget_optimization is not None:
        params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"
    
    # Add new parameters
    if buying_type:
        params["buying_type"] = buying_type

    if bid_strategy is not None:
        params["bid_strategy"] = getattr(bid_strategy, 'value', bid_strategy)
    
    if bid_cap is not None:
        params["bid_cap"] = str(bid_cap)
    
    if spend_cap is not None:
        params["spend_cap"] = str(spend_cap)
    
    
    if special_ad_category_country:
        params["special_ad_category_country"] = special_ad_category_country
    
    try:
        data = await make_api_request(endpoint, access_token, params, method="POST")
        
        # Ensure name field is included in the response
        if "name" not in data or not data.get("name"):
            data["name"] = name
        
        return json.dumps(data)
    except Exception as e:
        error_msg = str(e)
        return json.dumps({
            "error": "Failed to create campaign",
            "details": error_msg,
            "params_sent": params
        })


@mcp_server.tool()
@meta_api_tool
async def update_campaign(
    access_token: str = None,
    campaign_id: str = None,
    name: str = None,
    status: Optional[CampaignStatus] = None,
    special_ad_categories: List[str] = None,
    daily_budget = None,
    lifetime_budget = None,
    bid_strategy: Optional[CampaignBidStrategy] = None,
    bid_cap = None,
    spend_cap = None,
    campaign_budget_optimization: bool = None,
    objective: Optional[CampaignObjective] = None,
    use_adset_level_budgets: bool = None,
) -> str:
    """
    Update an existing campaign in a Meta Ads account.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        campaign_id: Meta Ads campaign ID (required)
        name: Campaign name
        status: Campaign status
        special_ad_categories: List of special ad categories if applicable
        daily_budget: Daily budget in account currency (in cents) as a string. Budgets are managed at the campaign level.
                     Set to empty string "" to remove the daily budget.
        lifetime_budget: Lifetime budget in account currency (in cents) as a string. Budgets are managed at the campaign level.
                        Set to empty string "" to remove the lifetime budget.
        bid_strategy: Bid strategy. If you enable campaign budget optimization, you should set bid_strategy at the parent campaign level.
                     If you do not enable campaign budget optimization, you should set bid_strategy at the ad set level.
        bid_cap: Bid cap in account currency (in cents) as a string
        spend_cap: Spending limit for the campaign in account currency (in cents) as a string, should be at least 10000 cents
        campaign_budget_optimization: Whether to enable campaign budget optimization.
        objective: Campaign objective (outcome-based). Note: May not always be updatable
        use_adset_level_budgets: If True, removes campaign-level budgets to switch to ad set level budgets
    """
    if not campaign_id:
        return json.dumps({"error": "No campaign ID provided"})

    endpoint = f"{campaign_id}"
    
    params = {}
    
    # Add parameters to the request only if they are provided
    if name is not None:
        params["name"] = name
    if status is not None:
        params["status"] = getattr(status, 'value', status)
    if special_ad_categories is not None:
        # Note: Updating special_ad_categories might have specific API rules or might not be allowed after creation.
        # The API might require an empty list `[]` to clear categories. Check Meta Docs.
        params["special_ad_categories"] = json.dumps(special_ad_categories)
    
    # Handle budget parameters based on use_adset_level_budgets setting
    if use_adset_level_budgets is not None:
        if use_adset_level_budgets:
            # Remove campaign-level budgets when switching to ad set level budgets
            params["daily_budget"] = ""
            params["lifetime_budget"] = ""
            if campaign_budget_optimization is not None:
                params["campaign_budget_optimization"] = "false"
        else:
            # If switching back to campaign-level budgets, use the provided budget values
            if daily_budget is not None:
                if daily_budget == "":
                    params["daily_budget"] = ""
                else:
                    params["daily_budget"] = str(daily_budget)
            if lifetime_budget is not None:
                if lifetime_budget == "":
                    params["lifetime_budget"] = ""
                else:
                    params["lifetime_budget"] = str(lifetime_budget)
            if campaign_budget_optimization is not None:
                params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"
    else:
        # Normal budget updates when not changing budget strategy
        if daily_budget is not None:
            # To remove budget, set to empty string
            if daily_budget == "":
                params["daily_budget"] = ""
            else:
                params["daily_budget"] = str(daily_budget)
        if lifetime_budget is not None:
            # To remove budget, set to empty string
            if lifetime_budget == "":
                params["lifetime_budget"] = ""
            else:
                params["lifetime_budget"] = str(lifetime_budget)
        if campaign_budget_optimization is not None:
            params["campaign_budget_optimization"] = "true" if campaign_budget_optimization else "false"
    
    if bid_strategy is not None:
        params["bid_strategy"] = getattr(bid_strategy, 'value', bid_strategy)
    if bid_cap is not None:
        params["bid_cap"] = str(bid_cap)
    if spend_cap is not None:
        params["spend_cap"] = str(spend_cap)
    if objective is not None:
        params["objective"] = getattr(objective, 'value', objective)

    if not params:
        return json.dumps({"error": "No update parameters provided"})

    try:
        # Use POST method for updates as per Meta API documentation
        data = await make_api_request(endpoint, access_token, params, method="POST")
        
        # Ensure campaign_id is included in the response
        data["campaign_id"] = campaign_id
        
        # Fetch campaign details to ensure name is included in response
        campaign_details = await make_api_request(campaign_id, access_token, {"fields": "name"})
        if "name" in campaign_details:
            data["name"] = campaign_details["name"]
        
        # Add a note about budget strategy if switching to ad set level budgets
        if use_adset_level_budgets is not None and use_adset_level_budgets:
            data["budget_strategy"] = "ad_set_level"
            data["note"] = "Campaign updated to use ad set level budgets. Set budgets when creating ad sets within this campaign."
        
        return json.dumps(data)
    except Exception as e:
        error_msg = str(e)
        # Include campaign_id in error for better context
        return json.dumps({
            "error": f"Failed to update campaign {campaign_id}",
            "details": error_msg,
            "params_sent": params # Be careful about logging sensitive data if any
        }) 


@mcp_server.tool()
@meta_api_tool
async def get_complete_campaign_details_deep(access_token: str = None, account_id: str = None, campaign_id: str = None, name_contains: str = None) -> str:
    """
    Get comprehensive campaign information including campaign details, ad sets, ads, and ad creatives.
    This tool orchestrates multiple API calls to provide a complete view of a campaign's structure.

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (required, format: act_XXXXXXXXX)
        campaign_id: Meta Ads campaign ID (alternative to name_contains)
        name_contains: Search for campaigns containing this text in their name (alternative to campaign_id)
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"})

    if not campaign_id and not (name_contains and name_contains.strip()):
        return json.dumps({"error": "Either campaign_id or name_contains must be provided"})

    campaign_details = await get_campaign_details(access_token=access_token, account_id=account_id, campaign_id=campaign_id, name_contains=name_contains)
    campaign_data = json.loads(campaign_details)

    if "error" in campaign_data:
        return campaign_details

    if name_contains and ("data" not in campaign_data or not campaign_data.get("data")):
        return json.dumps({"error": "Campaign not found"})

    if name_contains and "data" in campaign_data and len(campaign_data["data"]) > 1:
        campaign_names = [camp.get("name", "Unknown") for camp in campaign_data["data"]]
        return json.dumps({
            "error": f"Search returned {len(campaign_data['data'])} campaigns instead of 1. Found campaigns: {', '.join(campaign_names)}. Please refine your search to target a single campaign."
        })

    if name_contains and "data" in campaign_data and campaign_data["data"]:
        actual_campaign = campaign_data["data"][0]
        actual_campaign_id = actual_campaign["id"]
    elif campaign_id:
        actual_campaign_id = campaign_id
        if "data" not in campaign_data:
            campaign_data = {"data": [campaign_data]}
    else:
        return json.dumps({"error": "Campaign not found"})

    result = {
        "campaign": campaign_data,
        "adsets": {"data": []},
        "ads": {"data": []},
        "ad_creatives": {"data": []},
        "errors": []
    }

    from .adsets import get_adsets, get_adset_details
    from .ads import get_ads, get_ad_creatives

    adsets_response = await get_adsets(access_token=access_token, account_id=account_id, campaign_id=actual_campaign_id)
    adsets_data = json.loads(adsets_response)

    if "error" in adsets_data:
        result["errors"].append(f"Adsets API error: {adsets_data.get('error', 'Unknown error')}")
        result["adsets"]["error"] = "Error retrieving ad sets"
    elif "data" in adsets_data and adsets_data["data"]:
        result["adsets"] = adsets_data

        detailed_adsets = []
        for adset in adsets_data["data"]:
            adset_details_response = await get_adset_details(access_token=access_token, adset_id=adset["id"])
            adset_details_data = json.loads(adset_details_response)
            if "error" not in adset_details_data:
                detailed_adsets.append(adset_details_data)
            else:
                result["errors"].append(f"Adset details error for {adset.get('id', 'unknown')}: {adset_details_data.get('error', 'Unknown error')}")

        if detailed_adsets:
            result["adsets"]["data"] = detailed_adsets
    else:
        result["adsets"]["error"] = "No ad sets found"

    ads_response = await get_ads(access_token=access_token, account_id=account_id, campaign_id=actual_campaign_id)
    ads_data = json.loads(ads_response)

    if "error" in ads_data:
        result["errors"].append(f"Ads API error: {ads_data.get('error', 'Unknown error')}")
        result["ads"]["error"] = "Error retrieving ads"
        result["ad_creatives"]["error"] = "Cannot retrieve ad creatives due to ads error"
    elif "data" in ads_data and ads_data["data"]:
        result["ads"] = ads_data

        all_creatives = []
        for ad in ads_data["data"]:
            creatives_response = await get_ad_creatives(access_token=access_token, ad_id=ad["id"])
            creatives_data = json.loads(creatives_response)
            if "error" not in creatives_data and "data" in creatives_data:
                all_creatives.extend(creatives_data["data"])
            else:
                result["errors"].append(f"Ad creatives error for ad {ad.get('id', 'unknown')}: {creatives_data.get('error', 'Unknown error')}")

        if all_creatives:
            result["ad_creatives"]["data"] = all_creatives
    else:
        result["ads"]["error"] = "No ads found"
        result["ad_creatives"]["error"] = "No ads available to retrieve creatives"

    if not result["errors"]:
        del result["errors"]

    return json.dumps(result)


@mcp_server.tool()
@meta_api_tool
async def get_campaign_data_with_insights(
    access_token: str = None,
    account_id: str = None,
    campaign_id: str = None,
    name_contains: str = None,
    date_preset: str = "last_7d",
    campaign_insights_fields: str = None,
    ad_insights_fields: str = None
) -> str:
    """
    Get comprehensive campaign information including campaign details, ad sets, ads, ad creatives,
    and performance insights for both campaign and ad levels.

    This tool combines campaign structure data with performance metrics including:
    - Campaign level: spend, leads, cost per lead
    - Ad level: spend, leads, cost per lead, clicks, landing page views, CTR

    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        account_id: Meta Ads account ID (required, format: act_XXXXXXXXX)
        campaign_id: Meta Ads campaign ID (alternative to name_contains)
        name_contains: Search for campaigns containing this text in their name (alternative to campaign_id)
        date_preset: Time range preset (default: last_7d). Options: today, yesterday, last_3d, last_7d,
                    last_14d, last_28d, last_30d, last_90d, this_month, last_month, maximum, data_maximum, etc.
        campaign_insights_fields: Comma-separated list of fields for campaign insights (required)
                                 Example: "spend,actions,cost_per_action_type"
        ad_insights_fields: Comma-separated list of fields for ad insights (required)
                           Example: "ad_id,ad_name,spend,clicks,ctr,actions,cost_per_action_type"
    """
    if not account_id:
        return json.dumps({"error": "account_id is required"})

    if not campaign_id and not (name_contains and name_contains.strip()):
        return json.dumps({"error": "Either campaign_id or name_contains must be provided"})

    if not campaign_insights_fields:
        return json.dumps({"error": "campaign_insights_fields is required"})

    if not ad_insights_fields:
        return json.dumps({"error": "ad_insights_fields is required"})

    campaign_details = await get_complete_campaign_details_deep(
        access_token=access_token,
        account_id=account_id,
        campaign_id=campaign_id,
        name_contains=name_contains
    )
    campaign_data = json.loads(campaign_details)

    if "error" in campaign_data:
        return campaign_details

    if name_contains and "data" in campaign_data.get("campaign", {}):
        actual_campaign_id = campaign_data["campaign"]["data"][0]["id"]
    elif campaign_id:
        actual_campaign_id = campaign_id
    else:
        return json.dumps({"error": "Campaign not found"})

    batch_requests = [
        {
            "method": "GET",
            "relative_url": f"{actual_campaign_id}/insights?fields={campaign_insights_fields}&date_preset={date_preset}&time_increment=all_days&level=campaign"
        },
        {
            "method": "GET",
            "relative_url": f"{actual_campaign_id}/insights?fields={ad_insights_fields}&date_preset={date_preset}&time_increment=all_days&level=ad&limit=100"
        }
    ]

    batch_response = await make_batch_api_request(batch_requests, access_token)

    campaign_insights = {"data": []}
    ad_insights = {"data": []}

    if batch_response and len(batch_response) >= 2:
        if batch_response[0].get("code") == 200:
            try:
                campaign_insights = json.loads(batch_response[0].get("body", "{}"))
                if "data" in campaign_insights:
                    for insight in campaign_insights["data"]:
                        if "actions" in insight:
                            insight["actions"] = [a for a in insight["actions"] if a.get("action_type") in ["lead", "landing_page_view"]]
                        if "cost_per_action_type" in insight:
                            insight["cost_per_action_type"] = [a for a in insight["cost_per_action_type"] if a.get("action_type") == "lead"]
            except json.JSONDecodeError:
                campaign_insights = {"error": "Failed to parse campaign insights"}
        else:
            campaign_insights = {
                "error": f"Batch request failed with code {batch_response[0].get('code')}",
                "details": batch_response[0].get("body", "No response body")
            }

        if batch_response[1].get("code") == 200:
            try:
                ad_insights = json.loads(batch_response[1].get("body", "{}"))
                if "data" in ad_insights:
                    for insight in ad_insights["data"]:
                        if "actions" in insight:
                            insight["actions"] = [a for a in insight["actions"] if a.get("action_type") in ["lead", "landing_page_view"]]
                        if "cost_per_action_type" in insight:
                            insight["cost_per_action_type"] = [a for a in insight["cost_per_action_type"] if a.get("action_type") == "lead"]
            except json.JSONDecodeError:
                ad_insights = {"error": "Failed to parse ad insights"}
        else:
            ad_insights = {
                "error": f"Batch request failed with code {batch_response[1].get('code')}",
                "details": batch_response[1].get("body", "No response body")
            }

    result = {
        "campaign_data": campaign_data,
        "campaign_insights": campaign_insights,
        "ad_insights": ad_insights
    }

    return json.dumps(result)
