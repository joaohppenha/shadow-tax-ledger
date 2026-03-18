import json
import boto3
from dataclasses import dataclass
from datetime import datetime

ENDPOINT = "http://localhost:4566"
QUEUE_NFE_RAW = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/nfe-raw"
QUEUE_REFORMA  = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/reforma-rules"

# Alíquotas IBS/CBS por NCM (tabela simplificada — base para expansão via API)
# Fonte: PLP 68/2024 — alíquotas de referência
ALIQUOTAS_REFORMA = {
    "84713012": {"ibs": 0.178, "cbs": 0.088, "is": 0.00, "descricao": "Computadores"},
    "85171231": {"ibs": 0.178, "cbs": 0.088, "is": 0.00, "descricao": "Smartphones"},
    "62034200": {"ibs": 0.178, "cbs": 0.088, "is": 0.00, "descricao": "Vestuário"},
    "90211000": {"ibs": 0.120, "cbs": 0.060, "is": 0.00, "descricao": "Dispositivos médicos"},
    "87032310": {"ibs": 0.178, "cbs": 0.088, "is": 0.25, "descricao": "Automóveis"},
    "22021000": {"ibs": 0.178, "cbs": 0.088, "is": 0.20, "descricao": "Bebidas"},
    "default":  {"ibs": 0.178, "cbs": 0.088, "is": 0.00, "descricao": "Geral"},
}

@dataclass
class NotaSombra:
    chave_acesso_origem: str
    numero_nfe_origem: str
    cnpj_emitente: str
    data_calculo: str
    valor_total_nf: float

    # Carga atual (modelo fragmentado)
    icms_atual: float
    pis_atual: float
    cofins_atual: float
    ipi_atual: float
    carga_tributaria_atual: float

    # Carga futura (IVA dual)
    ibs_futuro: float
    cbs_futuro: float
    is_futuro: float
    carga_tributaria_futura: float

    # Delta — custo de transição
    delta_absoluto: float
    delta_percentual: float
    favoravel: bool

    def to_dict(self) -> dict:
        return self.__dict__

def calcular_carga_atual(itens: list) -> tuple:
    icms = pis = cofins = ipi = 0.0
    for item in itens:
        base = item["valor_total"]
        icms   += base * item["aliquota_icms"]   / 100
        pis    += base * item["aliquota_pis"]    / 100
        cofins += base * item["aliquota_cofins"] / 100
        ipi    += base * item.get("aliquota_ipi", 0) / 100
    return round(icms, 2), round(pis, 2), round(cofins, 2), round(ipi, 2)

def calcular_carga_futura(itens: list) -> tuple:
    ibs = cbs = is_ = 0.0
    for item in itens:
        ncm = item["ncm"]
        base = item["valor_total"]
        aliq = ALIQUOTAS_REFORMA.get(ncm, ALIQUOTAS_REFORMA["default"])
        # IVA dual: crédito pleno — incide sobre valor total
        ibs  += base * aliq["ibs"]
        cbs  += base * aliq["cbs"]
        is_  += base * aliq["is"]
    return round(ibs, 2), round(cbs, 2), round(is_, 2)

def processar_mensagem(body: dict) -> NotaSombra:
    itens = body["itens"]
    valor_total = body["valor_total_nf"]

    icms, pis, cofins, ipi = calcular_carga_atual(itens)
    ibs, cbs, is_ = calcular_carga_futura(itens)

    carga_atual  = round(icms + pis + cofins + ipi, 2)
    carga_futura = round(ibs + cbs + is_, 2)
    delta_abs    = round(carga_futura - carga_atual, 2)
    delta_pct    = round((delta_abs / carga_atual * 100) if carga_atual else 0, 2)

    return NotaSombra(
        chave_acesso_origem=body["chave_acesso"],
        numero_nfe_origem=body["numero"],
        cnpj_emitente=body["cnpj_emitente"],
        data_calculo=datetime.now().isoformat(),
        valor_total_nf=valor_total,
        icms_atual=icms,
        pis_atual=pis,
        cofins_atual=cofins,
        ipi_atual=ipi,
        carga_tributaria_atual=carga_atual,
        ibs_futuro=ibs,
        cbs_futuro=cbs,
        is_futuro=is_,
        carga_tributaria_futura=carga_futura,
        delta_absoluto=delta_abs,
        delta_percentual=delta_pct,
        favoravel=delta_abs < 0,
    )

def publicar_nota_sombra(nota: NotaSombra):
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sqs.send_message(
        QueueUrl=QUEUE_REFORMA,
        MessageBody=json.dumps(nota.to_dict()),
    )

def main():
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    print("Processando NF-es da fila nfe-raw...\n")
    processadas = 0

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_NFE_RAW,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        mensagens = response.get("Messages", [])
        if not mensagens:
            break

        for msg in mensagens:
            body = json.loads(msg["Body"])
            nota_sombra = processar_mensagem(body)
            publicar_nota_sombra(nota_sombra)

            sinal = "-" if nota_sombra.favoravel else "+"
            print(
                f"NF-e {nota_sombra.numero_nfe_origem} | "
                f"Atual: R$ {nota_sombra.carga_tributaria_atual:>12,.2f} | "
                f"Futura: R$ {nota_sombra.carga_tributaria_futura:>12,.2f} | "
                f"Delta: {sinal}R$ {abs(nota_sombra.delta_absoluto):>10,.2f} ({nota_sombra.delta_percentual:+.1f}%)"
            )

            sqs.delete_message(
                QueueUrl=QUEUE_NFE_RAW,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            processadas += 1

    print(f"\n{processadas} NF-e(s) processadas → notas sombra publicadas em reforma-rules!")

if __name__ == "__main__":
    main()
