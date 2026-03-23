-- ========================================
-- TABELA BRONZE (dados crus)
-- ========================================

CREATE TABLE IF NOT EXISTS bronze.transacoes (
    id SERIAL PRIMARY KEY,
    transacao_id VARCHAR(100),
    cliente_id INT,
    produto VARCHAR(100),
    categoria VARCHAR(100),
    valor NUMERIC(10,2),
    quantidade INT,
    data_transacao TIMESTAMP,
    metodo_pagamento VARCHAR(50),
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);