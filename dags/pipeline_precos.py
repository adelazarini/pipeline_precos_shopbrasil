"""Pipeline diario de precos por categoria - ShopBrasil.

Topologias:
  linear  : ingestao -> analise -> persistencia
  fan-out : separar_por_categoria -> N x calcular_metricas (Dynamic Task Mapping)
  fan-in  : N x calcular_metricas -> consolidar
"""
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group

from common import api, db
from common.callbacks import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
    sla_miss_callback,
)
from shopbrasil_operators.validar_produtos import ValidarProdutosOperator

log = logging.getLogger(__name__)


@dag(
    dag_id="pipeline_precos_shopbrasil",
    schedule="0 6 * * *", #rodar todos dias as 6 da manhã
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "pricing"],
    default_args={"owner": "data-team"},
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
)
def pipeline_precos():

    # ---------------- INGESTAO ----------------
    @task_group(group_id="ingestao")
    def ingestao():
        @task(
            task_id="buscar_produtos",
            retries=4,
            retry_delay=timedelta(seconds=10),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            sla=timedelta(minutes=30),
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
        )
        def buscar_produtos() -> list[dict]:
            try:
                return api.get_produtos()
            except Exception as e:
                log.error("Falha ao buscar produtos: %s", e)
                raise  # sem o raise o Airflow nao aciona o retry

        produtos = buscar_produtos()
        validados = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)
        return validados.output

    # ---------------- ANALISE ----------------
    @task_group(group_id="analise")
    def analise(produtos):
        @task
        def separar_por_categoria(produtos: list[dict]) -> list[dict]:
            """Fan-out: uma entrada por categoria (novas categorias entram sozinhas)."""
            grupos: dict[str, list[float]] = {}
            for p in produtos:
                grupos.setdefault(p["category"], []).append(float(p["price"]))
            return [{"categoria": c, "precos": v} for c, v in sorted(grupos.items())]

        @task(pool="ecommerce_pool")
        def calcular_metricas(grupo: dict) -> dict:
            precos = grupo["precos"]
            return {
                "categoria": grupo["categoria"],
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": min(precos),
                "preco_maximo": max(precos),
                "qtd_produtos": len(precos),
            }

        @task
        def consolidar(metricas) -> list[dict]:
            """Fan-in: junta os resultados de todas as tasks mapeadas."""
            return list(metricas)

        grupos = separar_por_categoria(produtos)
        metricas = calcular_metricas.expand(grupo=grupos)
        return consolidar(metricas)

    # ---------------- PERSISTENCIA ----------------
    @task_group(group_id="persistencia")
    def persistencia(metricas):
        @task
        def salvar_snapshot(metricas: list[dict], ds=None):
            db.upsert_snapshot(metricas, ds)
            log.info("Snapshot de %s gravado (%d categorias)", ds, len(metricas))

        @task
        def salvar_historico(metricas: list[dict], logical_date=None):
            db.inserir_historico(metricas, logical_date.isoformat())
            log.info("Historico gravado (%d categorias)", len(metricas))

        salvar_snapshot(metricas)
        salvar_historico(metricas)

    produtos = ingestao()
    metricas = analise(produtos)
    persistencia(metricas)


pipeline_precos()
