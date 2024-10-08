-- CTE FB and Google
with facebook_date as (
select
	coalesce (ad_date) as ad_date,
	coalesce (url_parameters,'no data') as url_parameters,
	'facebook' as source,
	coalesce (spend,0) as spend,
	coalesce (impressions,0) as impressions,
	coalesce (reach,0) as reach,
	coalesce (clicks,0) as clicks,
	coalesce (leads,0) as leads,
	coalesce (value,0) as value
from facebook_ads_basic_daily fabd
left join
	facebook_adset fa on fa.adset_id = fabd.adset_id
left join
	facebook_campaign fc on fc.campaign_id= fabd.campaign_id
union all
select
   coalesce (ad_date) as ad_date ,
	coalesce (url_parameters,'no data') as url_parameters ,
	'google' as source,
    coalesce (spend,0) as spend,
	coalesce (impressions,0) as impressions ,
	coalesce (reach,0) as reach,
	coalesce (clicks,0) as clicks ,
	coalesce (leads,0) as leads ,
    coalesce (value,0) as value
from google_ads_basic_daily gabd
),
--New 2 CTE with ad_mohth
  month_data  as (
select
date_trunc('month',ad_date) as ad_month,
regexp_match(url_parameters,'utm_campaign=([^&]*)'::text) as utm_campaign,
sum (spend) as total_spend,
sum(impressions)as total_impressions,
sum(clicks) as total_clicks,
sum(value) as total_value,
        case when sum(impressions) > 0 then (sum (clicks)*100)/ ((sum(impressions))::numeric) else 0 end as ctr,
        case when sum(clicks) > 0 then sum (spend) / (sum(clicks)::numeric) else 0 end as  cpc,
        case when sum(impressions) > 0 then  (sum (spend) * 1000 )/ (sum(impressions))::numeric else 0 end as cpm,
        case WHEN sum(spend) > 0 then (sum(value) - sum(spend)) / (sum(spend)::numeric) else 0 end as romi
from facebook_date
group by ad_month, utm_campaign
 ),
 --3 CTE
 lag_data as(
 select*,
 lag(romi) over (partition by utm_campaign order by ad_month desc) as previous_month_romi,
 lag(ctr)  over (partition by utm_campaign order by ad_month desc) as previous_month_ctr,
 lag(cpm)  over (partition by utm_campaign order by ad_month desc) as previous_month_cpm
 from month_data
 )
 select
  ad_month,
  utm_campaign,
  total_spend,
  total_impressions,
  total_clicks,
  total_value,
  ctr,
  cpc,
  cpm,
  romi,
 case when previous_month_cpm != 0 then ((cpm - previous_month_cpm)/previous_month_cpm)*100 else 0 end as cpm_change,
 case when previous_month_ctr != 0 then ((ctr - previous_month_ctr) / previous_month_ctr) * 100 else 0 end as ctr_change,
 case when previous_month_romi != 0 then ((romi - previous_month_romi) / previous_month_romi) * 100 else 0 end as romi_change
from lag_data;
