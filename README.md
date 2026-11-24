# ShopBrasil - Pipeline de precos (Airflow)

Pipeline diario (06:00 America/Sao_Paulo) que coleta produtos da FakeStore API,
calcula metricas por categoria (medio, minimo, maximo, quantidade) e grava no PostgreSQL analitico.

## Como subir
```bash
cp .env.example .env          # no Linux: echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
```
- UI: http://localhost:8080 (airflow / airflow)
- Postgres analitico: localhost:5433 (analytics / analytics / db analytics)
- Pool `ecommerce_pool` (2 slots) e Connection `postgres_analytics` sao criados automaticamente.

Ative o DAG `pipeline_precos_shopbrasil` e dispare manualmente (Trigger DAG).

## Verificar resultado
```bash
docker compose exec postgres-analytics psql -U analytics -c "SELECT * FROM metricas_categoria;"
docker compose exec postgres-analytics psql -U analytics -c "SELECT * FROM metricas_categoria_historico;"
```

## Arquitetura
- **Linear:** ingestao -> analise -> persistencia (TaskGroups)
- **Fan-out:** `separar_por_categoria` -> N x `calcular_metricas` (`.expand`, pool `ecommerce_pool`)
- **Fan-in:** N x `calcular_metricas` -> `consolidar`
- **Resiliencia:** `buscar_produtos` com 4 retries, exponential backoff, try/except + raise, 3 callbacks, SLA de 30 min
- **Idempotencia:** snapshot com `PRIMARY KEY (categoria, data_referencia)` + `ON CONFLICT DO UPDATE`;
  historico com `UNIQUE (categoria, data_execucao)` + `ON CONFLICT DO NOTHING`. Re-rodar a mesma run nao duplica.
- **Opcionais:** `ValidarProdutosOperator`, tabela de historico, SLA + alerta simulado.

## Estrutura
```
dags/pipeline_precos.py        # DAG
dags/common/{api,db,callbacks}.py
plugins/shopbrasil_operators/validar_produtos.py
sql/init_analytics.sql
docker-compose.yaml
```

## Testes sugeridos
- **Retry:** troque a URL em `dags/common/api.py` por uma invalida e veja backoff + callbacks nos logs.
- **Pool:** adicione `time.sleep(10)` em `calcular_metricas` e confira no Gantt que so 2 rodam por vez.
- **Idempotencia:** limpe (Clear) a run e confirme que o `count(*)` nao muda.
