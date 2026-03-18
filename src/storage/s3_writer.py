import json
import io
import boto3
import pandas as pd
from datetime import datetime

ENDPOINT = "http://localhost:4566"
QUEUE_REFORMA = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/reforma-rules"

def get_sqs():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

def salvar_parquet(registros: list, bucket: str, prefixo: str):
    df = pd.DataFrame(registros)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    agora = datetime.now()
    chave = (
        f"{prefixo}/"
        f"ano={agora.year}/mes={agora.month:02d}/dia={agora.day:02d}/"
        f"notas_sombra_{agora.strftime('%H%M%S')}.parquet"
    )

    get_s3().put_object(
        Bucket=bucket,
        Key=chave,
        Body=buffer.getvalue(),
    )
    print(f"Salvo em s3://{bucket}/{chave}")
    return chave

def main():
    sqs = get_sqs()
    registros = []

    print("Lendo notas sombra da fila reforma-rules...\n")

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_REFORMA,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        mensagens = response.get("Messages", [])
        if not mensagens:
            break

        for msg in mensagens:
            body = json.loads(msg["Body"])
            registros.append(body)
            sqs.delete_message(
                QueueUrl=QUEUE_REFORMA,
                ReceiptHandle=msg["ReceiptHandle"],
            )

    if not registros:
        print("Nenhuma mensagem na fila.")
        return

    print(f"{len(registros)} notas sombra lidas.\n")

    # Salva notas sombra
    salvar_parquet(registros, "shadow-tax-shadow", "notas-sombra")

    # Salva relatório de delta (só os campos financeiros)
    campos_delta = [
        "numero_nfe_origem", "cnpj_emitente", "data_calculo",
        "valor_total_nf", "carga_tributaria_atual", "carga_tributaria_futura",
        "delta_absoluto", "delta_percentual", "favoravel",
    ]
    delta = [{k: r[k] for k in campos_delta} for r in registros]
    salvar_parquet(delta, "shadow-tax-delta", "delta-report")

    # Resumo
    favoraveis = sum(1 for r in registros if r["favoravel"])
    delta_total = sum(r["delta_absoluto"] for r in registros)
    print(f"\nResumo do lote:")
    print(f"  NF-es processadas : {len(registros)}")
    print(f"  Carga reduz       : {favoraveis}")
    print(f"  Carga aumenta     : {len(registros) - favoraveis}")
    print(f"  Delta total       : R$ {delta_total:+,.2f}")

if __name__ == "__main__":
    main()
