"""Insights and Reporting functionality for Meta Ads API."""

import json
from typing import Optional, List, Union
from .api import meta_api_tool, make_api_request
from .utils import download_image, try_multiple_download_methods, ad_creative_images, create_resource_from_image
from .campaigns import get_campaigns
from .server import mcp_server
import base64
import datetime


@mcp_server.tool()
@meta_api_tool
async def get_insights(access_token: str = None, object_id: str = None,
                      date_preset: str = "last_30d", breakdown: str = "",
                      level: str = "ad", limit: int = 10, after: str = "",
                      campaign_ids: Optional[List[str]] = None,
                      time_range: Optional[dict] = None,
                      time_increment: Optional[Union[str, int]] = "all_days") -> str:
    """
    Get performance insights for a campaign, ad set, ad or account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        object_id: ID of the campaign, ad set, ad or account
        date_preset: Relative time range preset. Default: last_30d. Options:
                     today, yesterday, this_month, last_month, this_quarter, maximum, data_maximum, last_3d,
                     last_7d, last_14d, last_28d, last_30d, last_90d, last_week_mon_sun, last_week_sun_sat,
                     last_quarter, last_year, this_week_mon_today, this_week_sun_today, this_year.
                     Ignored if time_range is provided.
        breakdown: Optional breakdown dimension. Accepts a single value or a
                   comma-separated list (e.g. "publisher_platform,platform_position").
                   Note: When using "platform_position", you must also include
                   "publisher_platform" or the API may return an error.
                   Valid values include:
                   Demographic: age, gender, country, region, dma
                   Platform/Device: device_platform, platform_position, publisher_platform, impression_device
                   Creative Assets: ad_format_asset, body_asset, call_to_action_asset, description_asset, 
                                  image_asset, link_url_asset, title_asset, video_asset, media_asset_url,
                                  media_creator, media_destination_url, media_format, media_origin_url,
                                  media_text_content, media_type, creative_relaxation_asset_type,
                                  flexible_format_asset_type, gen_ai_asset_type
                   Campaign/Ad Attributes: breakdown_ad_objective, breakdown_reporting_ad_id, app_id, product_id
                   Conversion Tracking: coarse_conversion_value, conversion_destination, standard_event_content_type,
                                       signal_source_bucket, is_conversion_id_modeled, fidelity_type, redownload
                   Time-based: hourly_stats_aggregated_by_advertiser_time_zone, 
                              hourly_stats_aggregated_by_audience_time_zone, frequency_value
                   Extensions/Landing: ad_extension_domain, ad_extension_url, landing_destination, 
                                      mdsa_landing_destination
                   Attribution: sot_attribution_model_type, sot_attribution_window, sot_channel, 
                               sot_event_type, sot_source
                   Mobile/SKAN: skan_campaign_id, skan_conversion_id, skan_version, postback_sequence_index
                   CRM/Business: crm_advertiser_l12_territory_ids, crm_advertiser_subvertical_id,
                                crm_advertiser_vertical_id, crm_ult_advertiser_id, user_persona_id, user_persona_name
                   Advanced: hsid, is_auto_advance, is_rendered_as_delayed_skip_ad, mmm, place_page_id,
                            marketing_messages_btn_name, impression_view_time_advertiser_hour_v2, comscore_market,
                            comscore_market_code
        level: Level of aggregation (ad, adset, campaign, account)
        limit: Maximum number of results to return (default: 10)
        after: Pagination cursor to get the next set of results
        campaign_ids: Internal field, do not use
        time_range: {"since": "YYYY-MM-DD", "until": "YYYY-MM-DD"}
                    A single absolute time range. UNIX timestamps are not supported.
                    When provided, overrides date_preset. Ignored if time_ranges is provided.
        time_increment: "monthly", "all_days" or integer (1-90)
                        Default: "all_days". If integer, number of days to slice results.
                        Ignored if time_ranges is specified.
    Returns:
        JSON response containing insights data
    """
    if not object_id:
        return json.dumps({"error": "No object ID provided"})
        
    endpoint = f"{object_id}/insights"
    params = {
        "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,cpc,cpm,ctr,reach,frequency,actions,action_values,conversions,unique_clicks,cost_per_action_type,date_start,date_stop",
        "level": level,
        "limit": limit
    }

    if time_range:
        if isinstance(time_range, dict):
            params["time_range"] = json.dumps(time_range)
        else:
            params["time_range"] = time_range
    else:
        params["date_preset"] = date_preset
    
    if time_increment is not None:
        params["time_increment"] = time_increment
    
    if breakdown:
        params["breakdowns"] = breakdown
    
    if campaign_ids is not None:
        filtering = [{"field": "campaign.id", "operator": "IN", "value": campaign_ids}]
        params["filtering"] = json.dumps(filtering)
    
    if after:
        params["after"] = after
    
    data = await make_api_request(endpoint, access_token, params)
    
    return json.dumps(data)


@mcp_server.tool()
@meta_api_tool
async def get_insights_summary(access_token: str = None, object_id: str = None, 
                              date_preset: str = "last_30d", breakdown: str = "", 
                              level: str = "ad", campaign_ids: Optional[List[str]] = None) -> str:
    """
    Get aggregated performance summary for a campaign, ad set, ad or account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        object_id: ID of the campaign, ad set, ad or account
        date_preset: Preset time range string. Options: today, yesterday, this_month, last_month, this_quarter, 
                    maximum, data_maximum, last_3d, last_7d, last_14d, last_28d, last_30d, last_90d, 
                    last_week_mon_sun, last_week_sun_sat, last_quarter, last_year, this_week_mon_today, 
                    this_week_sun_today, this_year
        breakdown: Optional breakdown dimension. Accepts a single value or a
                   comma-separated list (e.g. "publisher_platform,platform_position").
                   Note: When using "platform_position", you must also include
                   "publisher_platform" or the API may return an error.
                   Valid values include:
                   Demographic: age, gender, country, region, dma
                   Platform/Device: device_platform, platform_position, publisher_platform, impression_device
                   Creative Assets: ad_format_asset, body_asset, call_to_action_asset, description_asset, 
                                  image_asset, link_url_asset, title_asset, video_asset, media_asset_url,
                                  media_creator, media_destination_url, media_format, media_origin_url,
                                  media_text_content, media_type, creative_relaxation_asset_type,
                                  flexible_format_asset_type, gen_ai_asset_type
                   Campaign/Ad Attributes: breakdown_ad_objective, breakdown_reporting_ad_id, app_id, product_id
                   Conversion Tracking: coarse_conversion_value, conversion_destination, standard_event_content_type,
                                       signal_source_bucket, is_conversion_id_modeled, fidelity_type, redownload
                   Time-based: hourly_stats_aggregated_by_advertiser_time_zone, 
                              hourly_stats_aggregated_by_audience_time_zone, frequency_value
                   Extensions/Landing: ad_extension_domain, ad_extension_url, landing_destination, 
                                      mdsa_landing_destination
                   Attribution: sot_attribution_model_type, sot_attribution_window, sot_channel, 
                               sot_event_type, sot_source
                   Mobile/SKAN: skan_campaign_id, skan_conversion_id, skan_version, postback_sequence_index
                   CRM/Business: crm_advertiser_l12_territory_ids, crm_advertiser_subvertical_id,
                                crm_advertiser_vertical_id, crm_ult_advertiser_id, user_persona_id, user_persona_name
                   Advanced: hsid, is_auto_advance, is_rendered_as_delayed_skip_ad, mmm, place_page_id,
                            marketing_messages_btn_name, impression_view_time_advertiser_hour_v2, comscore_market,
                            comscore_market_code
        level: Level of aggregation (ad, adset, campaign, account)
        campaign_ids: internal field, do not use
    Returns:
        JSON response containing aggregated summary with:
        - total_spend: Sum of all spend values across all results
        - total_leads: Sum of all lead actions across all results  
        - active_campaigns: Count of active campaigns (filtered by campaign_ids if provided)
        - paused_campaigns: Count of paused campaigns (filtered by campaign_ids if provided)
    """
    if not object_id:
        return json.dumps({"error": "No object ID provided"})
        
    endpoint = f"{object_id}/insights"
    params = {
        "fields": "account_id,spend,actions",
        "level": level,
        "limit": 1000  # Get all data for aggregation, no pagination
    }
    
    params["date_preset"] = date_preset
    
    if breakdown:
        params["breakdowns"] = breakdown
    
    if campaign_ids is not None:
        filtering = [{"field": "campaign.id", "operator": "IN", "value": campaign_ids}]
        params["filtering"] = json.dumps(filtering)
    
    data = await make_api_request(endpoint, access_token, params)
    
    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list) and 'error' not in data:
        total_spend = 0.0
        total_leads = 0
        
        for record in data['data']:
            if 'spend' in record and record['spend']:
                try:
                    total_spend += float(record['spend'])
                except (ValueError, TypeError):
                    pass
            
            if 'actions' in record and isinstance(record['actions'], list):
                for action in record['actions']:
                    if action.get('action_type') == 'lead' and action.get('value'):
                        try:
                            total_leads += int(action['value'])
                        except (ValueError, TypeError):
                            pass
        
        aggregated_results = {
            'total_spend': round(total_spend, 2),
            'total_leads': total_leads
        }
        
        account_id = None
        
        if data['data']:
            first_record = data['data'][0]
            raw_account_id = first_record.get('account_id')
            if raw_account_id and not raw_account_id.startswith('act_'):
                account_id = f"act_{raw_account_id}"
            else:
                account_id = raw_account_id
        
        if not account_id and object_id:
            if object_id.startswith('act_'):
                account_id = object_id
            
        if account_id:
            try:
                campaigns_endpoint = f"{account_id}/campaigns"
                campaigns_params = {
                    "fields": "id,status",
                    "effective_status": json.dumps(["ACTIVE", "PAUSED"]),
                    "limit": 1000
                }

                campaigns_data = await make_api_request(campaigns_endpoint, access_token, campaigns_params)

                if isinstance(campaigns_data, dict) and 'data' in campaigns_data and isinstance(campaigns_data['data'], list):
                    active_count = 0
                    paused_count = 0

                    for campaign in campaigns_data['data']:
                        campaign_id = campaign.get('id')
                        campaign_status = campaign.get('status')

                        if campaign_ids is not None and campaign_id not in campaign_ids:
                            continue

                        if campaign_status == 'ACTIVE':
                            active_count += 1
                        elif campaign_status == 'PAUSED':
                            paused_count += 1

                    aggregated_results['active_campaigns'] = active_count
                    aggregated_results['paused_campaigns'] = paused_count
                        
            except Exception as e:
                import traceback
                error_details = {
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'account_id': account_id,
                    'traceback': traceback.format_exc()
                }
                aggregated_results['campaign_count_error'] = error_details
        
        return json.dumps(aggregated_results)
    else:
        if isinstance(data, dict) and 'error' in data:
            return json.dumps(data)
        else:
            return json.dumps({"error": "No valid data received", "raw_response": data})





 
