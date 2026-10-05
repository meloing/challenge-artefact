-- ============================================================================
-- WABA Group - Architecture Lambda : Requête Unifiée Trino SQL
-- Conforme aux spécifications du Niveau 3 (Page 11-12)
--
-- Combine :
--   1. Batch Layer : Historique consolidé depuis Apache Iceberg (gold.daily_transaction_volume)
--   2. Speed Layer : Événements temps réel récents depuis Kafka (silver-bank-transactions)
-- ============================================================================

-- 1. Requête Canonique Lambda Unifiée
SELECT
  COALESCE(b.country_code, s.country_code) AS country,
  COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
  ROUND(COALESCE(b.total_amount_eur, 0.0) + COALESCE(s.streaming_amount_eur, 0.0), 2) AS total_eur,
  COALESCE(b.total_transactions, 0) + COALESCE(s.streaming_txn_count, 0) AS total_transactions,
  CASE 
    WHEN s.streaming_amount_eur > 0 THEN 'HYBRID_BATCH_STREAM'
    ELSE 'HISTORICAL_BATCH_ONLY'
  END AS data_freshness_status
FROM iceberg.gold.daily_transaction_volume b
FULL OUTER JOIN (
    -- Agrégation en temps réel des flux récents sur Kafka
    SELECT
      country_code,
      CAST(from_iso8601_timestamp(timestamp) AS DATE) AS event_time,
      COUNT(*) AS streaming_txn_count,
      SUM(amount_eur) AS streaming_amount_eur
    FROM kafka.default."silver-bank-transactions"
    WHERE from_iso8601_timestamp(timestamp) >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
    GROUP BY country_code, CAST(from_iso8601_timestamp(timestamp) AS DATE)
) s
  ON b.country_code = s.country_code
  AND b.txn_date = s.event_time
ORDER BY 1, 2;


-- 2. Vue Synthétique Unifiée du Solde et de l'Activité par Pays
WITH batch_totals AS (
    SELECT
      country_code,
      SUM(total_amount_eur) AS batch_volume_eur,
      SUM(total_transactions) AS batch_txns
    FROM iceberg.gold.daily_transaction_volume
    GROUP BY country_code
),
stream_totals AS (
    SELECT
      country_code,
      SUM(amount_eur) AS stream_volume_eur,
      COUNT(*) AS stream_txns
    FROM kafka.default."silver-bank-transactions"
    GROUP BY country_code
)
SELECT
  COALESCE(b.country_code, s.country_code) AS country_code,
  COALESCE(b.batch_txns, 0) AS historical_transactions,
  COALESCE(s.stream_txns, 0) AS realtime_streaming_transactions,
  COALESCE(b.batch_txns, 0) + COALESCE(s.stream_txns, 0) AS total_combined_transactions,
  ROUND(COALESCE(b.batch_volume_eur, 0.0) + COALESCE(s.stream_volume_eur, 0.0), 2) AS total_consolidated_volume_eur
FROM batch_totals b
FULL OUTER JOIN stream_totals s ON b.country_code = s.country_code
ORDER BY total_consolidated_volume_eur DESC;
