"""Insights and Reporting functionality for Meta Ads API."""

import json
from typing import Optional, Union, Dict
from .api import meta_api_tool, make_api_request
from .utils import download_image, try_multiple_download_methods, ad_creative_images, create_resource_from_image
from .campaigns import get_campaigns
from .server import mcp_server
import base64
import datetime


@mcp_server.tool()
@meta_api_tool
async def get_insights(access_token: str = None, object_id: str = None, 
                      time_range: Union[str, Dict[str, str]] = "maximum", breakdown: str = "", 
                      level: str = "ad") -> str:
    """
    Get performance insights for a campaign, ad set, ad or account.
    
    Args:
        access_token: Meta API access token (optional - will use cached token if not provided)
        object_id: ID of the campaign, ad set, ad or account
        time_range: Either a preset time range string or a dictionary with "since" and "until" dates in YYYY-MM-DD format
                   Preset options: today, yesterday, this_month, last_month, this_quarter, maximum, data_maximum, 
                   last_3d, last_7d, last_14d, last_28d, last_30d, last_90d, last_week_mon_sun, 
                   last_week_sun_sat, last_quarter, last_year, this_week_mon_today, this_week_sun_today, this_year
                   Dictionary example: {"since":"2023-01-01","until":"2023-01-31"}
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
    
    Key Fields Returned:
        Object Identifiers:
        - account_id, account_name: Ad account identification
        - campaign_id, campaign_name: Campaign level identification  
        - adset_id, adset_name: Ad set level identification
        - ad_id, ad_name: Individual ad identification
        - date_start, date_stop: Date range for the data
        
        Basic Performance Metrics:
        - impressions: Number of times ads were displayed
        - reach: Number of unique users who saw ads
        - clicks: Total number of clicks on ads
        - spend: Total amount spent on ads
        - frequency: Average number of times each person saw ads
        
        Engagement Rates & Costs:
        - ctr: Click-through rate (clicks/impressions)
        - cpc: Cost per click
        - cpm: Cost per thousand impressions
        - unique_clicks: Number of unique users who clicked
        
        Advanced Cost Metrics:
        - cost_per_action_type: Cost per specific action types
        - cost_per_conversion: Cost per conversion event
        - cost_per_thruplay: Cost per ThruPlay (15+ second video view)
        - cost_per_2_sec_continuous_video_view: Cost per 2-second video view
        - cost_per_estimated_ad_recallers: Cost per estimated ad recall
        - cost_per_inline_link_click: Cost per inline link click
        - cost_per_inline_post_engagement: Cost per inline post engagement
        - cost_per_outbound_click: Cost per outbound click
        - cost_per_unique_action_type: Cost per unique action type
        - cost_per_unique_click: Cost per unique click
        - cost_per_unique_inline_link_click: Cost per unique inline link click
        
        Video Performance Metrics:
        - video_play_curve_actions: Video play curve data
        - video_p25_watched_actions: 25% video completion actions
        - video_p50_watched_actions: 50% video completion actions  
        - video_p75_watched_actions: 75% video completion actions
        - video_p95_watched_actions: 95% video completion actions
        - video_p100_watched_actions: 100% video completion actions
        - video_30_sec_watched_actions: 30-second video view actions
        - video_avg_time_watched_actions: Average video watch time
        
        Click & Engagement Metrics:
        - outbound_clicks: Clicks to destinations off Meta platforms
        - inline_link_clicks: Clicks on links within ads
        - inline_post_engagement: Engagement with inline posts
        - social_spend: Spend on social actions
        - unique_inline_link_clicks: Unique inline link clicks
        - website_ctr: Website click-through rate
        
        Canvas & Instant Experience Metrics:
        - canvas_avg_view_time: Average Canvas view time
        - canvas_avg_view_percent: Average Canvas view percentage
        - instant_experience_clicks_to_open: Clicks to open Instant Experience
        - instant_experience_clicks_to_start: Clicks to start Instant Experience
        - instant_experience_outbound_clicks: Outbound clicks from Instant Experience
        
        Return on Ad Spend (ROAS):
        - mobile_app_purchase_roas: Mobile app purchase ROAS
        - purchase_roas: Overall purchase ROAS
        - website_purchase_roas: Website purchase ROAS
        
        Brand Awareness & Quality Metrics:
        - estimated_ad_recall_rate: Estimated ad recall rate
        - estimated_ad_recallers: Estimated number of ad recallers
        - quality_score_ectr: Expected click-through rate quality score
        - quality_score_ecvr: Expected conversion rate quality score
        - conversion_rate_ranking: Conversion rate ranking
        - engagement_rate_ranking: Engagement rate ranking
        - quality_ranking: Overall quality ranking
        
        
        Campaign Configuration:
        - attribution_setting: Attribution window settings
        - objective: Campaign objective
        - full_view_impressions: Full viewable impressions
        - full_view_reach: Full viewable reach
        
        Action Data:
        - actions: Array of action types and counts
        - action_values: Values associated with actions
        - conversions: Total conversion events
    
    Returns:
        JSON response containing insights data with aggregated results:
        - Original insights data in 'data' array
        - 'aggregated_results' object with:
          - total_spend: Sum of all spend values across all results
          - total_leads: Sum of all lead actions across all results
          - active_campaigns: Count of active campaigns in the account
          - paused_campaigns: Count of paused campaigns in the account
    """
    if not object_id:
        return json.dumps({"error": "No object ID provided"})
        
    endpoint = f"{object_id}/insights"
    params = {
        "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,date_start,date_stop,impressions,reach,clicks,spend,ctr,cpc,cpm,frequency,actions,action_values,conversions,unique_clicks,cost_per_action_type,cost_per_conversion,cost_per_thruplay,cost_per_2_sec_continuous_video_view,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_outbound_click,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,video_play_curve_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_30_sec_watched_actions,video_avg_time_watched_actions,outbound_clicks,inline_link_clicks,inline_post_engagement,social_spend,unique_inline_link_clicks,website_ctr,canvas_avg_view_time,canvas_avg_view_percent,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,mobile_app_purchase_roas,purchase_roas,website_purchase_roas,estimated_ad_recall_rate,estimated_ad_recallers,quality_score_ectr,quality_score_ecvr,attribution_setting,conversion_rate_ranking,engagement_rate_ranking,quality_ranking,full_view_impressions,full_view_reach,objective",
        "level": level
    }
    
    # Handle time range based on type
    if isinstance(time_range, dict):
        # Use custom date range with since/until parameters
        if "since" in time_range and "until" in time_range:
            params["time_range"] = json.dumps(time_range)
        else:
            return json.dumps({"error": "Custom time_range must contain both 'since' and 'until' keys in YYYY-MM-DD format"})
    else:
        # Use preset date range
        params["date_preset"] = time_range
    
    if breakdown:
        params["breakdowns"] = breakdown
    
    data = await make_api_request(endpoint, access_token, params)
    
    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list) and 'error' not in data:
        total_spend = 0.0
        total_leads = 0
        active_campaigns = 0
        paused_campaigns = 0
        
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
        
        account_id = None
        
        if data['data']:
            first_record = data['data'][0]
            account_id = first_record.get('account_id')
        
        if not account_id and object_id:
            if object_id.startswith('act_'):
                account_id = object_id
            
        if account_id:
            try:
                active_campaigns_response = await get_campaigns(access_token, account_id, 1000, "ACTIVE")
                active_campaigns_data = json.loads(active_campaigns_response)
                if 'data' in active_campaigns_data and isinstance(active_campaigns_data['data'], list):
                    active_campaigns = len(active_campaigns_data['data'])
                
                paused_campaigns_response = await get_campaigns(access_token, account_id, 1000, "PAUSED")
                paused_campaigns_data = json.loads(paused_campaigns_response)
                if 'data' in paused_campaigns_data and isinstance(paused_campaigns_data['data'], list):
                    paused_campaigns = len(paused_campaigns_data['data'])
                    
            except Exception as e:
                # If campaign counting fails, log it but don't break the main response
                # Campaign counts will remain 0
                pass
        
        # Add aggregated results to the response
        data['aggregated_results'] = {
            'total_spend': round(total_spend, 2),
            'total_leads': total_leads,
            'active_campaigns': active_campaigns,
            'paused_campaigns': paused_campaigns
        }
    
    return json.dumps(data)





 