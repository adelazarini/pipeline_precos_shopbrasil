-- Snapshot idempotente: 1 linha por (categoria, dia logico da run)
CREATE TABLE IF NOT EXISTS metricas_categoria (
    categoria        TEXT        NOT NULL,
    data_referencia  DATE        NOT NULL,
    preco_medio      NUMERIC(10,2),
    preco_minimo     NUMERIC(10,2),
    preco_maximo     NUMERIC(10,2),
    qtd_produtos     INTEGER,
    atualizado_em    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (categoria, data_referencia)
);

-- Historico (append) para acompanhar evolucao de precos.
-- UNIQUE garante que reprocessar a mesma run nao duplica.
CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id               BIGSERIAL PRIMARY KEY,
    categoria        TEXT        NOT NULL,
    data_execucao    TIMESTAMPTZ NOT NULL,
    preco_medio      NUMERIC(10,2),
    preco_minimo     NUMERIC(10,2),
    preco_maximo     NUMERIC(10,2),
    qtd_produtos     INTEGER,
    UNIQUE (categoria, data_execucao)
);
