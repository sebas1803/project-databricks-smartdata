USE CATALOG retail_ecommerce_prod;

-- Revocar SELECT
REVOKE SELECT ON ALL TABLES IN SCHEMA retail_ecommerce_prod.golden FROM `DataEngineers`;
REVOKE SELECT ON ALL TABLES IN SCHEMA retail_ecommerce_prod.silver FROM `DataEngineers`;

-- Revocar USAGE en schemas
REVOKE USAGE ON SCHEMA retail_ecommerce_prod.golden FROM `DataEngineers`;
REVOKE USAGE ON SCHEMA retail_ecommerce_prod.silver  FROM `DataEngineers`;
REVOKE USAGE ON SCHEMA retail_ecommerce_prod.bronze  FROM `DataEngineers`;

-- Revocar USAGE en catálogo
REVOKE USAGE ON CATALOG retail_ecommerce_prod FROM `DataEngineers`;
