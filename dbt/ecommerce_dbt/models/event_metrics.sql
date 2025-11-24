SELECT
  user_id,
  COUNT_IF(event_type = 'view') AS total_views,
  COUNT_IF(event_type = 'add_to_cart') AS total_adds,
  COUNT_IF(event_type = 'purchase') AS total_purchases
FROM {{ ref('events_raw') }}
GROUP BY user_id
