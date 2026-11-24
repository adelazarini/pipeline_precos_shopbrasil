from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema minimo dos produtos antes do processamento."""

    template_fields = ("produtos",)
    CAMPOS = {"id": int, "title": str, "price": (int, float), "category": str}

    def __init__(self, produtos, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context):
        erros = []
        for p in self.produtos:
            for campo, tipo in self.CAMPOS.items():
                if campo not in p or not isinstance(p[campo], tipo):
                    erros.append(f"produto {p.get('id')}: campo '{campo}' invalido")
            if isinstance(p.get("price"), (int, float)) and p["price"] < 0:
                erros.append(f"produto {p.get('id')}: preco negativo")
        if erros:
            raise ValueError("Schema invalido:\n" + "\n".join(erros))
        self.log.info("%d produtos validados", len(self.produtos))
        return self.produtos
