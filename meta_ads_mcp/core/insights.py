"""Insights and Reporting functionality for Meta Ads API."""

import json
from typing import Optional
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
                      level: str = "ad") -> str:
    """
    Get performance insights for a campaign, ad set, ad or account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        object_id: ID of the campaign, ad set, ad or account
        date_preset: Preset time range string. Options: today, yesterday, this_month, last_month, this_quarter, 
                    maximum, data_maximum, last_3d, last_7d, last_14d, last_28d, last_30d, last_90d, 
                    last_week_mon_sun, last_week_sun_sat, last_quarter, last_year, this_week_mon_today, 
                    this_week_sun_today, this_year
        breakdown: Optional breakdown dimension. Valid values include:
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
    Returns:
        JSON response containing insights data with aggregated results:
        - Original insights data in 'data' array
        - 'aggregated_results' object with:
          - total_spend: Sum of all spend values across all results
          - total_leads: Sum of all lead actions across all results
          - active_campaigns: Count of active campaigns (only included when account_id is available)
          - paused_campaigns: Count of paused campaigns (only included when account_id is available)
    """
    if not object_id:
        return json.dumps({"error": "No object ID provided"})
        
    endpoint = f"{object_id}/insights"
    params = {
        "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,cpc,cpm,ctr,reach,frequency,actions,action_values,conversions,unique_clicks,cost_per_action_type",
        "level": level
    }
    
    params["date_preset"] = date_preset
    
    if breakdown:
        params["breakdowns"] = breakdown
    
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
                active_campaigns_response = await get_campaigns(access_token=access_token, account_id=account_id, limit=1000, status_filter="ACTIVE")
                active_campaigns_data = json.loads(active_campaigns_response)
                if 'data' in active_campaigns_data and isinstance(active_campaigns_data['data'], list):
                    aggregated_results['active_campaigns'] = len(active_campaigns_data['data'])
                
                paused_campaigns_response = await get_campaigns(access_token=access_token, account_id=account_id, limit=1000, status_filter="PAUSED")
                paused_campaigns_data = json.loads(paused_campaigns_response)
                if 'data' in paused_campaigns_data and isinstance(paused_campaigns_data['data'], list):
                    aggregated_results['paused_campaigns'] = len(paused_campaigns_data['data'])
                    
            except Exception as e:
                import traceback
                error_details = {
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'account_id': account_id,
                    'traceback': traceback.format_exc()
                }
                
                if 'active_campaigns_response' in locals():
                    error_details['active_response_type'] = type(active_campaigns_response).__name__
                    error_details['active_response_sample'] = str(active_campaigns_response)[:300] + ('...' if len(str(active_campaigns_response)) > 300 else '')
                else:
                    error_details['active_response_type'] = 'not_fetched'
                
                if 'paused_campaigns_response' in locals():
                    error_details['paused_response_type'] = type(paused_campaigns_response).__name__  
                    error_details['paused_response_sample'] = str(paused_campaigns_response)[:300] + ('...' if len(str(paused_campaigns_response)) > 300 else '')
                else:
                    error_details['paused_response_type'] = 'not_fetched'
                
                aggregated_results['campaign_count_error'] = error_details
        
        data['aggregated_results'] = aggregated_results
    
    return json.dumps(data)





 