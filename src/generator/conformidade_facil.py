import json
import os
import urllib.request
from pathlib import Path

USE_REAL_API = os.getenv("USE_REAL_API", "false").lower() == "true"
API_URL = "https://cff.svrs.rs.gov.br/api/v1/consultas/classTrib"
TABELA_LOCAL = Path(__file__).parent / "data" / "ncm_reforma.json"

def carregar_tabela_local() -> dict:
    with open(TABELA_LOCAL, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["tabela"]

def buscar_api_real(ncm: str) -> dict:
    """
    Conecta na API Conformidade Fácil (requer certificado ICP-Brasil em producao).
    Documentacao: https://cff.svrs.rs.gov.br
    """
    url = f"{API_URL}?ncm={ncm}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())

def consultar_ncm(ncm: str) -> dict:
    if USE_REAL_API:
        try:
            return buscar_api_real(ncm)
        except Exception as e:
            print(f"  API real indisponível ({e}). Usando tabela local.")

    tabela = carregar_tabela_local()
    return tabela.get(ncm, {
        "descricao": "NCM nao mapeado",
        "ibs": 0.178,
        "cbs": 0.088,
        "is": 0.00,
        "reducao_base": 0.00,
        "regime": "geral",
    })

def listar_tabela() -> dict:
    return carregar_tabela_local()

if __name__ == "__main__":
    print(f"Modo: {'API real' if USE_REAL_API else 'tabela local'}\n")
    tabela = listar_tabela()
    for ncm, dados in tabela.items():
        print(
            f"NCM {ncm} | {dados['descricao']:<35} | "
            f"IBS {dados['ibs']*100:.1f}% | "
            f"CBS {dados['cbs']*100:.1f}% | "
            f"IS {dados['is']*100:.1f}% | "
            f"Regime: {dados['regime']}"
        )
