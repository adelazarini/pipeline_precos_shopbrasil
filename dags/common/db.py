"""Persistencia no Postgres analitico (todas as gravacoes sao idempotentes)."""
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "postgres_analytics"

DDL = """
CREATE TABLE IF NOT EXISTS metricas_categoria (
    categoria TEXT NOT NULL, data_referencia DATE NOT NULL,
    preco_medio NUMERIC(10,2), preco_minimo NUMERIC(10,2), preco_maximo NUMERIC(10,2),
    qtd_produtos INTEGER, atualizado_em TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (categoria, data_referencia)
);
CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id BIGSERIAL PRIMARY KEY, categoria TEXT NOT NULL, data_execucao TIMESTAMPTZ NOT NULL,
    preco_medio NUMERIC(10,2), preco_minimo NUMERIC(10,2), preco_maximo NUMERIC(10,2),
    qtd_produtos INTEGER, UNIQUE (categoria, data_execucao)
);
"""

UPSERT_SNAPSHOT = """
INSERT INTO metricas_categoria
    (categoria, data_referencia, preco_medio, preco_minimo, preco_maximo, qtd_produtos)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (categoria, data_referencia) DO UPDATE SET
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    qtd_produtos  = EXCLUDED.qtd_produtos,
    atualizado_em = now();
"""

INSERT_HISTORICO = """
INSERT INTO metricas_categoria_historico
    (categoria, data_execucao, preco_medio, preco_minimo, preco_maximo, qtd_produtos)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (categoria, data_execucao) DO NOTHING;
"""


def _executar(sql: str, linhas: list[tuple]) -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:  # transacao: commit ou rollback
            cur.execute(DDL)
            cur.executemany(sql, linhas)
    finally:
        conn.close()


def _linha(m: dict, data) -> tuple:
    return (m["categoria"], data, m["preco_medio"], m["preco_minimo"],
            m["preco_maximo"], m["qtd_produtos"])


def upsert_snapshot(metricas: list[dict], data_referencia: str) -> None:
    _executar(UPSERT_SNAPSHOT, [_linha(m, data_referencia) for m in metricas])


def inserir_historico(metricas: list[dict], data_execucao: str) -> None:
    _executar(INSERT_HISTORICO, [_linha(m, data_execucao) for m in metricas])
