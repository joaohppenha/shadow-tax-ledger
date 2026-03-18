import json
import boto3
import urllib.request
from datetime import datetime

ENDPOINT = "http://localhost:4566"
QUEUE_PLP = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/plp-status"

# PLPs relevantes da Reforma Tributária
PLPS_MONITORADOS = {
    "68": "IBS/CBS/IS — regras gerais do IVA dual",
    "108": "IBS — regulamentação estadual/municipal",
    "130": "CBS — regulamentação federal",
}

BASE_URL = "https://legis.senado.leg.br/dadosabertos/materia"

def buscar_status_plp(numero: str) -> dict:
    url = f"{BASE_URL}/pesquisa/lista?sigla=PLP&numero={numero}&ano=2024"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            materias = (
                data.get("PesquisaBasicaMateria", {})
                    .get("Materias", {})
                    .get("Materia", [])
            )
            if not materias:
                return _status_fallback(numero)

            # Pode retornar lista ou dict
            materia = materias[0] if isinstance(materias, list) else materias

            codigo = materia.get("CodigoMateria", "")
            return buscar_situacao(codigo, numero)

    except Exception as e:
        print(f"  Aviso: API indisponível para PLP {numero} ({e}). Usando fallback.")
        return _status_fallback(numero)

def buscar_situacao(codigo: str, numero: str) -> dict:
    url = f"{BASE_URL}/{codigo}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            materia = data.get("DetalheMateria", {}).get("Materia", {})
            situacao = (
                materia.get("SituacaoAtual", {})
                       .get("Autuacoes", {})
                       .get("Autuacao", {})
            )
            if isinstance(situacao, list):
                situacao = situacao[-1]

            return {
                "plp": numero,
                "descricao": PLPS_MONITORADOS.get(numero, ""),
                "situacao": situacao.get("DescricaoSituacao", "Não disponível"),
                "local": situacao.get("NomeLocal", "Não disponível"),
                "data_situacao": situacao.get("DataSituacao", ""),
                "lei_aprovada": _verificar_aprovacao(situacao),
                "timestamp": datetime.now().isoformat(),
                "fonte": "API Senado — dadosabertos.senado.leg.br",
            }
    except Exception:
        return _status_fallback(numero)

def _verificar_aprovacao(situacao: dict) -> bool:
    descricao = situacao.get("DescricaoSituacao", "").lower()
    return any(p in descricao for p in ["transformad", "promulgad", "sancionad", "publicad"])

def _status_fallback(numero: str) -> dict:
    """Dados de fallback baseados no status conhecido até março/2026."""
    fallbacks = {
        "68": {
            "situacao": "Transformado em Lei Complementar 214/2025",
            "local": "Presidência da República",
            "data_situacao": "2025-01-16",
            "lei_aprovada": True,
        },
        "108": {
            "situacao": "Em tramitação no Senado Federal",
            "local": "Senado Federal",
            "data_situacao": "2024-12-01",
            "lei_aprovada": False,
        },
        "130": {
            "situacao": "Em tramitação na Câmara dos Deputados",
            "local": "Câmara dos Deputados",
            "data_situacao": "2024-11-01",
            "lei_aprovada": False,
        },
    }
    fb = fallbacks.get(numero, {})
    return {
        "plp": numero,
        "descricao": PLPS_MONITORADOS.get(numero, ""),
        "situacao": fb.get("situacao", "Não disponível"),
        "local": fb.get("local", "Não disponível"),
        "data_situacao": fb.get("data_situacao", ""),
        "lei_aprovada": fb.get("lei_aprovada", False),
        "timestamp": datetime.now().isoformat(),
        "fonte": "fallback — dados de referência",
    }

def publicar_sqs(status: dict):
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sqs.send_message(
        QueueUrl=QUEUE_PLP,
        MessageBody=json.dumps(status),
    )

def main():
    print("Monitorando PLPs da Reforma Tributária...\n")

    for numero, descricao in PLPS_MONITORADOS.items():
        print(f"Consultando PLP {numero} — {descricao}")
        status = buscar_status_plp(numero)
        publicar_sqs(status)

        aprovado = "APROVADO — Lei vigente" if status["lei_aprovada"] else "EM TRAMITACAO"
        print(f"  Status   : {status['situacao']}")
        print(f"  Local    : {status['local']}")
        print(f"  Situacao : {aprovado}")
        print(f"  Fonte    : {status['fonte']}")
        print()

    print("Status publicados na fila plp-status!")

if __name__ == "__main__":
    main()
