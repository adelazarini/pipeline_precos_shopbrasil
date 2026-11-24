"""Acesso a API de catalogo (FakeStore)."""
import logging

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://fakestoreapi.com"


def get_produtos(timeout: int = 15) -> list[dict]:
    resp = requests.get(f"{BASE_URL}/products", timeout=timeout)
    resp.raise_for_status()
    produtos = resp.json()
    if not produtos:
        raise ValueError("API retornou lista vazia")
    log.info("%d produtos recebidos", len(produtos))
    return produtos
