"""Callbacks de ciclo de vida e alertas (simulados via log)."""
import logging

log = logging.getLogger(__name__)


def _info(context) -> str:
    ti = context["task_instance"]
    return (
        f"dag={ti.dag_id} task={ti.task_id} run={context.get('run_id')} "
        f"tentativa={ti.try_number}"
    )


def enviar_alerta(canal: str, mensagem: str) -> None:
    """Simula envio de alerta (troque por Slack/e-mail em producao)."""
    log.error("[ALERTA -> %s] %s", canal, mensagem)


def on_failure_callback(context):
    enviar_alerta("#data-alerts", f"FALHA DEFINITIVA | {_info(context)} | erro={context.get('exception')}")


def on_retry_callback(context):
    log.warning("RETRY | %s | erro=%s", _info(context), context.get("exception"))


def on_success_callback(context):
    log.info("SUCESSO | %s", _info(context))


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    enviar_alerta("#data-alerts", f"SLA ESTOURADO no DAG {dag.dag_id} | tasks={task_list}")
