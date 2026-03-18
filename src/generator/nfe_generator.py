import json
import random
import boto3
from faker import Faker
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional

fake = Faker("pt_BR")

ENDPOINT = "http://localhost:4566"
QUEUE_URL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/nfe-raw"

NCM_CODES = [
    "84713012", "85171231", "62034200",
    "90211000", "87032310", "22021000",
]

CFOP_CODES = ["5102", "5405", "6102", "6108"]

class ItemNFe(BaseModel):
    codigo_produto: str
    descricao: str
    ncm: str
    cfop: str
    quantidade: float
    valor_unitario: float
    valor_total: float
    aliquota_icms: float
    aliquota_pis: float
    aliquota_cofins: float
    aliquota_ipi: Optional[float] = 0.0

class NotaFiscal(BaseModel):
    chave_acesso: str = Field(default_factory=lambda: fake.numerify("#" * 44))
    numero: str = Field(default_factory=lambda: fake.numerify("######"))
    serie: str = "001"
    data_emissao: str = Field(default_factory=lambda: datetime.now().isoformat())
    cnpj_emitente: str = Field(default_factory=fake.cnpj)
    razao_social_emitente: str = Field(default_factory=fake.company)
    cnpj_destinatario: str = Field(default_factory=fake.cnpj)
    razao_social_destinatario: str = Field(default_factory=fake.company)
    uf_emitente: str = Field(default_factory=lambda: fake.estado_sigla())
    uf_destinatario: str = Field(default_factory=lambda: fake.estado_sigla())
    itens: list[ItemNFe] = Field(default_factory=list)
    valor_total_nf: float = 0.0
    valor_total_impostos: float = 0.0

def gerar_item() -> ItemNFe:
    ncm = random.choice(NCM_CODES)
    qtd = round(random.uniform(1, 100), 2)
    v_unit = round(random.uniform(10, 5000), 2)
    v_total = round(qtd * v_unit, 2)
    aliq_icms = random.choice([0.0, 7.0, 12.0, 17.0, 18.0, 25.0])
    aliq_pis = random.choice([0.65, 1.65])
    aliq_cofins = random.choice([3.0, 7.6])
    aliq_ipi = random.choice([0.0, 5.0, 10.0, 15.0])

    return ItemNFe(
        codigo_produto=fake.numerify("PROD-####"),
        descricao=fake.bs().title(),
        ncm=ncm,
        cfop=random.choice(CFOP_CODES),
        quantidade=qtd,
        valor_unitario=v_unit,
        valor_total=v_total,
        aliquota_icms=aliq_icms,
        aliquota_pis=aliq_pis,
        aliquota_cofins=aliq_cofins,
        aliquota_ipi=aliq_ipi,
    )

def gerar_nfe(num_itens: int = None) -> NotaFiscal:
    num_itens = num_itens or random.randint(1, 10)
    itens = [gerar_item() for _ in range(num_itens)]

    valor_total_nf = round(sum(i.valor_total for i in itens), 2)
    valor_total_impostos = round(sum(
        i.valor_total * (i.aliquota_icms + i.aliquota_pis + i.aliquota_cofins + i.aliquota_ipi) / 100
        for i in itens
    ), 2)

    return NotaFiscal(
        itens=itens,
        valor_total_nf=valor_total_nf,
        valor_total_impostos=valor_total_impostos,
    )

def publicar_sqs(nfe: NotaFiscal):
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=nfe.model_dump_json(),
    )

def main(quantidade: int = 5):
    print(f"Gerando {quantidade} NF-e(s)...\n")
    for i in range(quantidade):
        nfe = gerar_nfe()
        publicar_sqs(nfe)
        print(f"[{i+1}] NF-e {nfe.numero} | R$ {nfe.valor_total_nf:,.2f} | {len(nfe.itens)} itens | impostos R$ {nfe.valor_total_impostos:,.2f}")
    print(f"\n{quantidade} NF-e(s) publicadas na fila nfe-raw!")

if __name__ == "__main__":
    main(quantidade=10)
