import io
import json
import boto3
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

ENDPOINT = "http://localhost:4566"
QUEUE_PLP = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/plp-status"

st.set_page_config(
    page_title="Shadow Tax Ledger",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

CSS = (
    "<style>"
    "@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;700&display=swap');"
    "html,body,[class*='css']{font-family:'DM Sans',sans-serif!important;}"
    ".stApp{background-color:#F5F0E8;}"
    ".block-container{padding:2rem 2.5rem;max-width:1400px;}"
    ".dash-header{display:flex;align-items:flex-end;justify-content:space-between;border-bottom:2px solid #1A1A1A;padding-bottom:1rem;margin-bottom:2rem;}"
    ".dash-title{font-size:32px;font-weight:700;letter-spacing:-1px;color:#1A1A1A;line-height:1;margin:0;}"
    ".dash-sub{font-size:11px;letter-spacing:2px;text-transform:uppercase;color:#888;margin-top:6px;}"
    ".dash-date{font-family:'DM Mono',monospace;font-size:11px;color:#888;}"
    ".kpi-block{padding:1.25rem 1rem;}"
    ".kpi-black{background:#1A1A1A;}"
    ".kpi-blue{background:#2B5CE6;}"
    ".kpi-coral{background:#E85D24;}"
    ".kpi-amber{background:#E8A020;}"
    ".kpi-teal{background:#0D7C5F;}"
    ".kpi-label{font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:rgba(255,255,255,0.6);margin-bottom:8px;}"
    ".kpi-value{font-size:26px;font-weight:700;color:#fff;line-height:1;font-family:'DM Mono',monospace;}"
    ".kpi-delta{font-size:11px;color:rgba(255,255,255,0.6);margin-top:6px;font-family:'DM Mono',monospace;}"
    ".section-label{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#888;margin-bottom:1rem;margin-top:2rem;}"
    ".plp-card{background:#fff;border:1px solid #E0DAD0;padding:1.25rem;}"
    ".plp-num{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#888;}"
    ".plp-desc{font-size:13px;font-weight:500;color:#1A1A1A;margin:6px 0 10px;line-height:1.4;}"
    ".badge-ok{display:inline-block;background:#0D7C5F;color:#fff;font-size:10px;letter-spacing:1px;text-transform:uppercase;padding:3px 10px;font-weight:500;}"
    ".badge-pending{display:inline-block;background:#E8A020;color:#fff;font-size:10px;letter-spacing:1px;text-transform:uppercase;padding:3px 10px;font-weight:500;}"
    ".plp-meta{font-size:11px;color:#888;margin-top:8px;font-family:'DM Mono',monospace;}"
    "hr{border:none;border-top:1px solid #E0DAD0;margin:2rem 0;}"
    ".stButton>button{background:#1A1A1A!important;color:#fff!important;border:none!important;border-radius:0!important;font-size:10px!important;letter-spacing:1.5px!important;text-transform:uppercase!important;padding:8px 20px!important;}"
    ".stButton>button:hover{background:#2B5CE6!important;}"
    "</style>"
)

def get_s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")

def get_sqs():
    return boto3.client("sqs", endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")

@st.cache_data(ttl=30)
def carregar_delta():
    s3 = get_s3()
    try:
        objects = s3.list_objects_v2(Bucket="shadow-tax-delta", Prefix="delta-report/")
        arquivos = [o["Key"] for o in objects.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not arquivos:
            return pd.DataFrame()
        dfs = []
        for key in arquivos:
            obj = s3.get_object(Bucket="shadow-tax-delta", Key=key)
            dfs.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def carregar_plp_status():
    sqs = get_sqs()
    try:
        msgs = []
        while True:
            resp = sqs.receive_message(QueueUrl=QUEUE_PLP, MaxNumberOfMessages=10, WaitTimeSeconds=1)
            batch = resp.get("Messages", [])
            if not batch:
                break
            for m in batch:
                msgs.append(json.loads(m["Body"]))
        return msgs
    except Exception:
        return []

st.markdown(CSS, unsafe_allow_html=True)

now = datetime.now().strftime('%d.%m.%Y · %H:%M')
st.markdown(f'<div class="dash-header"><div><p class="dash-title">Shadow Tax Ledger</p><p class="dash-sub">Reforma Tributária · Espelhamento IBS / CBS</p></div><div class="dash-date">{now} · LC 214/2025</div></div>', unsafe_allow_html=True)

df = carregar_delta()
if not df.empty:
    df["numero_nfe_origem"] = df["numero_nfe_origem"].astype(str)

if df.empty:
    st.warning("Nenhum dado encontrado. Rode o pipeline primeiro.")
    st.code("python3 src/generator/nfe_generator.py\npython3 src/processor/shadow_calculator.py\npython3 src/storage/s3_writer.py")
    st.stop()

total_nfes     = len(df)
carga_atual    = df["carga_tributaria_atual"].sum()
carga_futura   = df["carga_tributaria_futura"].sum()
delta_total    = df["delta_absoluto"].sum()
pct_favoraveis = df["favoravel"].mean() * 100

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown(f'<div class="kpi-block kpi-black"><div class="kpi-label">NF-es analisadas</div><div class="kpi-value">{total_nfes}</div><div class="kpi-delta">lote atual</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="kpi-block kpi-blue"><div class="kpi-label">Carga atual</div><div class="kpi-value">R$ {carga_atual/1e6:.2f}M</div><div class="kpi-delta">ICMS · PIS · COFINS · IPI</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="kpi-block kpi-coral"><div class="kpi-label">Carga futura</div><div class="kpi-value">R$ {carga_futura/1e6:.2f}M</div><div class="kpi-delta">IBS · CBS · IS</div></div>', unsafe_allow_html=True)
with col4:
    sinal = "+" if delta_total > 0 else ""
    st.markdown(f'<div class="kpi-block kpi-amber"><div class="kpi-label">Delta total</div><div class="kpi-value">{sinal}R$ {abs(delta_total)/1e3:.0f}K</div><div class="kpi-delta">impacto do lote</div></div>', unsafe_allow_html=True)
with col5:
    st.markdown(f'<div class="kpi-block kpi-teal"><div class="kpi-label">Com redução</div><div class="kpi-value">{pct_favoraveis:.0f}%</div><div class="kpi-delta">{int(df["favoravel"].sum())} de {total_nfes} NF-es</div></div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

col_a, col_b = st.columns(2)

with col_a:
    st.markdown('<div class="section-label">Carga atual vs futura por NF-e</div>', unsafe_allow_html=True)
    fig = go.Figure()
    fig.add_bar(name="Carga atual", width=0.35,  x=df["numero_nfe_origem"].astype(str), y=df["carga_tributaria_atual"], marker_color="#2B5CE6")
    fig.add_bar(name="Carga futura", width=0.35, x=df["numero_nfe_origem"].astype(str), y=df["carga_tributaria_futura"], marker_color="#E85D24")
    fig.update_layout(barmode="group", plot_bgcolor="#fff", paper_bgcolor="#fff",
        font=dict(family="DM Sans", size=11, color="#888"),
        xaxis=dict(tickfont=dict(family="DM Mono", size=10), gridcolor="#F0EBE0"),
        yaxis=dict(tickfont=dict(family="DM Mono", size=10), gridcolor="#F0EBE0"),
        legend=dict(orientation="h", y=1.12, font=dict(size=11)),
        margin=dict(t=10, b=40, l=10, r=10), height=320)
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.markdown('<div class="section-label">Delta percentual por NF-e</div>', unsafe_allow_html=True)
    cores = ["#0D7C5F" if v else "#E85D24" for v in df["favoravel"]]
    fig2 = go.Figure()
    fig2.add_bar(x=df["numero_nfe_origem"], width=0.6, y=df["delta_percentual"], marker_color=cores)
    fig2.update_layout(plot_bgcolor="#fff", paper_bgcolor="#fff",
        font=dict(family="DM Sans", size=11, color="#888"),
        xaxis=dict(tickfont=dict(family="DM Mono", size=10), gridcolor="#F0EBE0"),
        yaxis=dict(tickfont=dict(family="DM Mono", size=10), gridcolor="#F0EBE0", ticksuffix="%"),
        margin=dict(t=10, b=40, l=10, r=10), height=320, showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)
st.markdown('<div class="section-label">Detalhamento por NF-e</div>', unsafe_allow_html=True)

df_display = df[["numero_nfe_origem","cnpj_emitente","carga_tributaria_atual","carga_tributaria_futura","delta_absoluto","delta_percentual","favoravel"]].copy()
df_display.columns = ["NF-e","CNPJ","Atual (R$)","Futura (R$)","Delta (R$)","Delta (%)","Redução?"]
df_display["Redução?"]    = df_display["Redução?"].map({True:"Sim", False:"Não"})
df_display["Atual (R$)"]  = df_display["Atual (R$)"].map("R$ {:,.2f}".format)
df_display["Futura (R$)"] = df_display["Futura (R$)"].map("R$ {:,.2f}".format)
df_display["Delta (R$)"]  = df_display["Delta (R$)"].map("R$ {:+,.2f}".format)
df_display["Delta (%)"]   = df_display["Delta (%)"].map("{:+.1f}%".format)
st.dataframe(df_display, use_container_width=True, hide_index=True)

st.markdown("<hr>", unsafe_allow_html=True)
st.markdown('<div class="section-label">Status legislativo dos PLPs</div>', unsafe_allow_html=True)

plps = carregar_plp_status()
if plps:
    cols = st.columns(len(plps))
    for i, plp in enumerate(plps):
        badge = "badge-ok" if plp.get("lei_aprovada") else "badge-pending"
        label = "Lei vigente" if plp.get("lei_aprovada") else "Em tramitação"
        with cols[i]:
            st.markdown(f'<div class="plp-card"><div class="plp-num">PLP {plp["plp"]}</div><div class="plp-desc">{plp["descricao"]}</div><span class="{badge}">{label}</span><div class="plp-meta">{plp["situacao"]}<br>{plp["local"]}</div></div>', unsafe_allow_html=True)
else:
    st.info("Rode o plp_monitor.py para carregar o status dos PLPs.")

st.markdown("<hr>", unsafe_allow_html=True)

col_f1, col_f2 = st.columns([4,1])
with col_f1:
    st.markdown(f'<span style="font-size:11px;color:#888;font-family:DM Mono,monospace">Atualiza automaticamente a cada 30s · LocalStack · LC 214/2025 · {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</span>', unsafe_allow_html=True)
with col_f2:
    if st.button("Atualizar agora"):
        st.cache_data.clear()
        st.rerun()
