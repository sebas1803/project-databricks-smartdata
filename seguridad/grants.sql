USE CATALOG retail_ecommerce_prod;

-- Dar acceso al catálogo al grupo DataEngineers
GRANT USAGE ON CATALOG retail_ecommerce_prod TO `DataEngineers`;

GRANT USAGE ON SCHEMA retail_ecommerce_prod.bronze  TO `DataEngineers`;
GRANT USAGE ON SCHEMA retail_ecommerce_prod.silver  TO `DataEngineers`;
GRANT USAGE ON SCHEMA retail_ecommerce_prod.golden TO `DataEngineers`;
