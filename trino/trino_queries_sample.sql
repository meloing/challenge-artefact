-- ============================================================================
-- WABA Group - Suite de Requêtes Analytiques Trino SQL (Levels 1 & 2)
-- Test et validation du Lakehouse Apache Iceberg
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Requêtes Analytiques de Base (Critères d'évaluation Level 1)
-- ----------------------------------------------------------------------------

-- A. Soldes par pays et par devise (Comptes bancaires)
SELECT 
    country_code,
    currency,
    COUNT(account_id) AS total_accounts,
    ROUND(SUM(balance), 2) AS total_balance_local,
    ROUND(AVG(balance), 2) AS avg_balance_local
FROM iceberg.bronze.accounts
GROUP BY country_code, currency
ORDER BY total_accounts DESC;

-- B. Volumes et montants des transactions par pays et ligne métier
SELECT 
    country_code,
    entity_type,
    transaction_status,
    COUNT(transaction_id) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount_local
FROM iceberg.bronze.bank_transactions
GROUP BY country_code, entity_type, transaction_status
ORDER BY country_code, transaction_count DESC;

-- C. Comptage des clients par entité et par segment
SELECT 
    country_code,
    entity_type,
    segment,
    COUNT(customer_id) AS customer_count,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_customers
FROM iceberg.bronze.customers
GROUP BY country_code, entity_type, segment
ORDER BY country_code, customer_count DESC;


-- ----------------------------------------------------------------------------
-- 2. Requêtes Réglementaires et KPIs Métier (Couche Gold - Level 2)
-- ----------------------------------------------------------------------------

-- D. Surveillance Ratio NPL (Prêts douteux) vs Seuil BCEAO (< 5%)
SELECT 
    country_code,
    loan_type,
    total_loans_count,
    defaulted_loans_count,
    ROUND(total_exposure_eur, 2) AS total_exposure_eur,
    ROUND(defaulted_exposure_eur, 2) AS defaulted_exposure_eur,
    ROUND(npl_ratio * 100, 2) AS npl_percentage,
    risk_status,
    CASE 
        WHEN npl_ratio <= 0.05 THEN '✅ CONFORME BCEAO'
        ELSE '⚠️ NON-CONFORME : EXCES DE RISQUE'
    END AS bceao_compliance_label
FROM iceberg.gold.npl_ratio_by_country
ORDER BY npl_ratio DESC;

-- E. Surveillance Loss Ratio Assurance (S/P) vs Seuil de vigilance CIMA (> 70%)
SELECT 
    month_year,
    country_code,
    product_line,
    ROUND(total_claims_eur, 2) AS total_claims_eur,
    ROUND(total_premiums_eur, 2) AS total_premiums_eur,
    ROUND(loss_ratio * 100, 2) AS loss_ratio_percentage,
    CASE 
        WHEN loss_ratio > 0.70 THEN '🔴 ALERTE CIMA : RATIO DEPASSE'
        WHEN loss_ratio >= 0.60 THEN '🟡 VIGILANCE RAPPROCHEE'
        ELSE '🟢 RENTABLE'
    END AS cima_supervision_status
FROM iceberg.gold.loss_ratio_by_product
ORDER BY loss_ratio DESC;

-- F. Analyse des Couloirs de Transferts Transfrontaliers Mobile Money
SELECT 
    week_year,
    corridor,
    sender_country,
    receiver_country,
    transfer_count,
    ROUND(total_volume_eur, 2) AS total_volume_eur,
    ROUND(avg_transfer_eur, 2) AS avg_transfer_eur
FROM iceberg.gold.cross_border_transfers
ORDER BY total_volume_eur DESC
LIMIT 10;

-- G. Évolution du Revenu Moyen Par Client (ARPC / ARPU)
SELECT 
    month_year,
    country_code,
    customer_segment,
    active_customers_count,
    ROUND(total_commissions_eur, 2) AS commissions_eur,
    ROUND(arpu_eur, 2) AS arpu_per_customer_eur
FROM iceberg.gold.customer_arpu_monthly
ORDER BY month_year DESC, arpu_eur DESC;
