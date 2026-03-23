-- ========================================
-- TABELA GOLD (agregações)
-- ========================================

CREATE TABLE IF NOT EXISTS gold.resumo_vendas (
    produto VARCHAR(100) PRIMARY KEY,
    total_vendas NUMERIC(10,2),
    total_quantidade INT
);