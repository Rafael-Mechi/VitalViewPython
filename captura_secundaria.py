import psutil
import pandas as pd
import os
import boto3
import time
import socket
from datetime import datetime, timezone
from psutil._common import bytes2human
import subprocess
from botocore.exceptions import BotoCoreError, ClientError

# from slack_sdk import WebClient
# from slack_sdk.errors import SlackApiError
# client = WebClient(token="Insira o Token aqui")

DATA_PATH = "DadosRecebidos"
CSV_PATH = "DadosRecebidos/captura_secundaria.csv"
os.makedirs(DATA_PATH, exist_ok=True)

S3_ENABLE = True
S3_BUCKET = "bucket-raw-2025-10-23-9773"
S3_OBJECT = f"captura_secundaria.csv"
s3 = boto3.client("s3")

def s3_chave_destino():
    return f"{S3_OBJECT}"

# Rede 
def obter_estatisticas_rede():
    estat = psutil.net_io_counters()
    return (
        estat.bytes_recv,      # Bytes recebidos
        estat.bytes_sent,      # Bytes enviados 
        estat.packets_recv,    # Pacotes recebidos 
        estat.packets_sent     # Pacotes enviados 
    )

def conexoes_tcp_ativas():
    try:
        return sum(
            conexao.status == psutil.CONN_ESTABLISHED
            for conexao in psutil.net_connections(kind='inet')
        )
    except Exception:
        return None

def latencia_tcp_ms(host="8.8.8.8", porta=53, timeout=1.5):
    inicio = time.monotonic()
    try:
        with socket.create_connection((host, porta), timeout=timeout):
            return (time.monotonic() - inicio) * 1000.0  # ms
    except Exception:
        return None

def ping_perda_e_rtt(host="8.8.8.8", count=4):
    try:
    
        resultado = subprocess.run(["ping", host, "-n", str(count)], capture_output=True, text=True).stdout

        perda_pct = None
        latencia_ms = None

        for linha in resultado.splitlines():
            linha_l = linha.lower()

            if "perdidos" in linha_l or "loss" in linha_l:
                perda_pct = float(linha.split("(")[1].split("%")[0])

            if "média" in linha_l or "average" in linha_l:
                val = ''.join([c for c in linha.split("=")[-1] if c.isdigit()])
                if val:
                    latencia_ms = float(val)

        return perda_pct, latencia_ms
    except:
        return None, None

# Estado para calcular down/up e pacotes por intervalo
prev_rx = prev_tx = prev_prx = prev_ptx = None
prev_t = None

# Inicializa disco anterior e intervalo
io_anterior = psutil.disk_io_counters()
INTERVALO_SEGUNDOS = 2


try:
    while True:

        linhas = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hostname = socket.gethostname()

        cpu_percent = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        mem_percent = mem.percent
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        
        
        # Rede 
        net_counters = psutil.net_io_counters()
        net_bytes_sent = int(net_counters.bytes_sent)
        net_bytes_recv = int(net_counters.bytes_recv)

        # Rede
        rx, tx, prx, ptx = obter_estatisticas_rede()
        agora = time.monotonic()

        if prev_t is None:
            net_down_mbps = net_up_mbps = 0.0
            pkts_in_interval = pkts_out_interval = 0
        else:
            dt = max(1e-6, agora - prev_t)
            net_down_mbps = ((rx - prev_rx) * 8) / dt / 1_000_000  # Mb/s
            net_up_mbps   = ((tx - prev_tx) * 8) / dt / 1_000_000  # Mb/s
            pkts_in_interval  = max(0, prx - prev_prx)
            pkts_out_interval = max(0, ptx - prev_ptx)

        prev_rx, prev_tx, prev_prx, prev_ptx, prev_t = rx, tx, prx, ptx, agora

        # Conexões e latência
        tcp_established = conexoes_tcp_ativas()
        perda_pct, rtt_ms_ping = ping_perda_e_rtt()
        net_latency_ms = rtt_ms_ping if rtt_ms_ping is not None else latencia_tcp_ms()

        # Coleta por processo
        for proc in psutil.process_iter(['name', 'username', 'pid', 'memory_percent', 'num_threads', 'create_time','status']):
            try:
                info = proc.info
                create_time_human = datetime.fromtimestamp(info.get('create_time')).strftime("%Y-%m-%d %H:%M:%S") \
                                    if info.get('create_time') else ""
                memory_human = bytes2human(proc.memory_info().rss)
                cpu_usage = proc.cpu_percent(interval=0.1)

                linhas.append({
                    "Nome da Máquina": hostname,
                    "Data da Coleta": timestamp,
                    "Processo": info.get('name'),
                    'Uso de CPU': cpu_percent,
                    'Uso de RAM': mem_percent,
                    'Uso de Disco': disk_percent,
                    "Uso de Threads": info.get('num_threads'),
                    "Quando foi iniciado": create_time_human,
                    "Status": info.get('status'),
                    #Rede
                    "Net bytes enviados": net_bytes_sent,
                    "Net bytes recebidos": net_bytes_recv,
                    "Net Down (Mbps)": round(net_down_mbps, 2),
                    "Net Up (Mbps)": round(net_up_mbps, 2),
                    "Pacotes IN (intervalo)": pkts_in_interval,
                    "Pacotes OUT (intervalo)": pkts_out_interval,
                    "Conexões TCP ESTABLISHED": tcp_established if tcp_established is not None else "",
                    "Latência (ms)": round(net_latency_ms, 1) if net_latency_ms is not None else "",
                    "Perda de Pacotes (%)": round(perda_pct, 1) if perda_pct is not None else "",
                })

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Salva CSV
        df = pd.DataFrame(linhas)
        # sem processos) 
        linha = {
            "Nome da Máquina": hostname,
            "Data da Coleta": timestamp,
            # Uso geral
            "Uso de CPU (%)": cpu_percent,
            "Uso de RAM (%)": mem_percent,
            "Uso de Disco (%)": disk_percent,
            # Rede
            "Net bytes enviados": net_bytes_sent,
            "Net bytes recebidos": net_bytes_recv,
            "Net Down (Mb/s)": round(net_down_mbps, 2),
            "Net Up (Mb/s)": round(net_up_mbps, 2),
            "Pacotes IN (intervalo)": pkts_in_interval,
            "Pacotes OUT (intervalo)": pkts_out_interval,
            "Conexões TCP ESTABLISHED": tcp_established if tcp_established is not None else "",
            "Latência (ms)": round(net_latency_ms, 1) if net_latency_ms is not None else "",
            "Perda de Pacotes (%)": round(perda_pct, 1) if perda_pct is not None else "",
        }

        df = pd.DataFrame([linha])
        if os.path.exists(CSV_PATH):
            df.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
        else:
            df.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)

        # S3
        if S3_ENABLE:
            try:
                destino = s3_chave_destino()
                s3.upload_file(CSV_PATH, S3_BUCKET, destino, ExtraArgs={"ContentType": "text/csv"})
                print(f"\n✅ S3 upload: s3://{S3_BUCKET}/{destino}")
            except (BotoCoreError, ClientError) as e:
                print(f"\n⚠️ Falha no upload S3: {e}")

        print(
            f"[{timestamp}] Host:{hostname} | "
            f"↓ {net_down_mbps:.2f} Mb/s ↑ {net_up_mbps:.2f} Mb/s | "
            f"Lat {net_latency_ms if net_latency_ms is not None else '—'} ms | "
            f"Perda {perda_pct if perda_pct is not None else '—'}% | "
            f"Conexões {tcp_established if tcp_established is not None else '—'}",
            end="\r", flush=True
        )

        time.sleep(10)  # intervalo entre coletas

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")
