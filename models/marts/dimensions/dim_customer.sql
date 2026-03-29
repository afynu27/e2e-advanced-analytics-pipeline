SELECT 
    ic.*,
    il.city_names, 
    il.state_names,
    il.state_full_name,
    il.geolocation_lat,
    il.geolocation_lng,
    COALESCE(scs.Segment, 'Non Buyer') AS rfm_segment,
    COALESCE(scs.Badge, 'Non Buyer') AS clv_badge,
    COALESCE(
    INITCAP(REPLACE(scs.Strategy, '_', ' ')),
    'Acquisition: First Purchase Incentive'
    ) AS sales_strategy,
    COALESCE(scs.Recency, 0) AS sales_recency, 
    COALESCE(scs.Frequency, 0) AS sales_frequency, 
    COALESCE(scs.Monetary, 0.0) AS sales_monetary, 
    COALESCE(scs.predicted_clv, 0.0) AS predicted_clv
FROM {{ ref('int_customer') }} ic
LEFT JOIN {{ ref('stg_customer_segments') }} scs ON ic.customer_sk = scs.customer_sk
LEFT JOIN {{ ref('int_location') }} il ON ic.location_id = il.location_id