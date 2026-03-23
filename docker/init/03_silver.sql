-- ========================================
-- TABELA SILVER (dados tratados)
-- ========================================

CREATE TABLE IF NOT EXISTS silver.transacoes_tratadas (
    id SERIAL PRIMARY KEY,
    transacao_id VARCHAR(100),
    cliente_id INT,
    produto VARCHAR(100),
    categoria VARCHAR(100),
    valor NUMERIC(10,2),
    quantidade INT,
    data_transacao TIMESTAMP,
    metodo_pagamento VARCHAR(50)
);