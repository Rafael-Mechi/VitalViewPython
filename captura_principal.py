import subprocess
import psutil
import pandas as pd
import os
import time
import socket
from datetime import datetime
import boto3


# ------------------------ Configuração básica ------------------------ #
DATA_PATH = "DadosRecebidos"
CSV_PATH = "DadosRecebidos/captura_principal.csv"
INTERVALO_SEGUNDOS = 2
os.makedirs(DATA_PATH, exist_ok=True)
s3 = boto3.client('s3')

bucket = "bucket-raw-2025-10-23-9773"
arquivo_local = CSV_PATH
destino_s3 = "captura_principal.csv"

# -------------------------- Funções utilitárias -------------------------- #

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
time.sleep(0.1) #delay
tempo_io_anterior = time.monotonic()


try:
    while True:
        
        inicio = time.monotonic()
        
        linhas = []

        data_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome_maquina = socket.gethostname()

        # ----------------------------- Métricas de host ----------------------------- #
        uso_cpu = psutil.cpu_percent(interval=1)
        
        # Memória principal.
        mem = psutil.virtual_memory()
        uso_memoria = mem.percent
        memoria_total_b = int(mem.total)
        memoria_usada_b = int(mem.used)

        # ------- Disco ------- #
        disco = psutil.disk_usage('/')
        uso_disco = disco.percent
        disco_total_b = int(disco.total)
        disco_usado_b = int(disco.used)
        disco_livre_b = int(disco.free)
        io_atual = psutil.disk_io_counters()
        tempo_io_atual = time.monotonic() # Marca o tempo atual
        # Calcula o tempo real decorrido desde a última medição
        dt_disco = max(1e-6, tempo_io_atual - tempo_io_anterior)
        
        # Taxa de transferência
        leitura_mb = float(io_atual.read_bytes - io_anterior.read_bytes) / (1024 * 1024)
        escrita_mb = float(io_atual.write_bytes - io_anterior.write_bytes) / (1024 * 1024)
        taxa_leitura = leitura_mb / dt_disco
        taxa_escrita = escrita_mb / dt_disco
        # Latência média
        total_leitura = io_atual.read_count - io_anterior.read_count
        tempo_leitura = io_atual.read_time - io_anterior.read_time
        total_escrita = io_atual.write_count - io_anterior.write_count
        tempo_escrita = io_atual.write_time - io_anterior.write_time

        latencia_leitura = (tempo_leitura / total_leitura) if total_leitura > 0 else 0
        latencia_escrita = (tempo_escrita / total_escrita) if total_escrita > 0 else 0

        # Atualiza o captura anterior como a atual
        io_anterior = io_atual
        tempo_io_anterior = tempo_io_atual

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

        # Uptime do sistema.
        tempo_boot = psutil.boot_time()
        uptime_segundos = int(time.time() - tempo_boot)

        # ----------------------------- CRIAÇÃO DA LINHA DE DADOS ----------------------------- #
        
        linha_dados = {
            "Data_da_Coleta": data_coleta,
            "Nome_da_Maquina": nome_maquina,
            
            # CPU
            "Uso_de_Cpu": uso_cpu,

            # Memória
            "Uso_de_RAM": uso_memoria,
            "RAM_total_(bytes)": memoria_total_b,
            "RAM_usada_(bytes)": memoria_usada_b,

            # Disco
            "Uso_de_Disco": uso_disco,
            "Disco_total_(bytes)": disco_total_b,
            "Disco_usado_(bytes)": disco_usado_b,
            "Disco_livre_(bytes)": disco_livre_b,
            "Disco_taxa_leitura_mbs": taxa_leitura,
            "Disco_taxa_escrita_mbs": taxa_escrita,
            "Disco_latencia_leitura": latencia_leitura,
            "Disco_latencia_escrita": latencia_escrita,
            
            # Rede
            "Net_Down_(Mbps)": net_down_mbps,
            "Net_Up_(Mbps)": net_up_mbps,
            "Pacotes_IN_(intervalo)": pkts_in_interval,
            "Pacotes_OUT_(intervalo)": pkts_out_interval,
            "Latencia_(ms)": net_latency_ms,
            "Perda_de_Pacotes_(%)": perda_pct,
            "Conexões_TCP_ESTABLISHED": tcp_established,
            "Net_bytes_enviados": net_bytes_sent,
            "Net_bytes_recebidos": net_bytes_recv,
            
            # Sistema
            "Uptime_(s)": uptime_segundos,
        }
        
        # Adiciona a linha de dados coletados à lista que será usada para criar o DataFrame
        linhas.append(linha_dados)

        df = pd.DataFrame(linhas)
        if os.path.exists(CSV_PATH):
            df.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
        else:
            df.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)

        print("Processos capturados com sucesso!")

        # s3.upload_file(arquivo_local, bucket, destino_s3)
        print("✅ Upload concluído com sucesso!")
        
        # Garante intervalo fixo
        fim = time.monotonic()
        dt = fim - inicio
        
        if dt < INTERVALO_SEGUNDOS:
            time.sleep(INTERVALO_SEGUNDOS - dt)

except KeyboardInterrupt:
    print("Monitoramento finalizado.")