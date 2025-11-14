import psutil
import pandas as pd
import os
import time
import socket
from datetime import datetime
import boto3


# ------------------------ Configuração básica ------------------------ #
DATA_PATH = "VitalViewPython/DadosRecebidos"
CSV_PATH = "VitalViewPython/DadosRecebidos/captura_principal.csv"
INTERVALO_SEGUNDOS = 2
os.makedirs(DATA_PATH, exist_ok=True)
s3 = boto3.client('s3')

bucket = "bucket-raw-2025-10-23-9773"
arquivo_local = CSV_PATH
destino_s3 = "captura_principal.csv"

# -------------------------- Funções utilitárias -------------------------- #
# Média de carga (load average): l1, l5, l15 indicam a média de tarefas
# prontas/executando (e em espera de I/O) nos últimos 1, 5 e 15 minutos.
# É a média de quantas tarefas estão disputando memória dentro da CPU
def carregar_media_carga():
    try:
        import os as _os
        l1, l5, l15 = _os.getloadavg()
        return l1, l5, l15
    except Exception:
        return 0.0, 0.0, 0.0


def obter_temperatura_cpu_c():
    try:
        temperaturas = psutil.sensors_temperatures(fahrenheit=False)
        for chave in ("coretemp", "cpu-thermal", "cpu_thermal"):
            if chave in temperaturas and temperaturas[chave]:
                return float(temperaturas[chave][0].current)
        for leituras in temperaturas.values():
            if leituras:
                return float(leituras[0].current)
    except Exception:
        pass
    return None

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


try:
    while True:
        linhas = []

        data_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome_maquina = socket.gethostname()

        # ----------------------------- Métricas de host ----------------------------- #
        uso_cpu = psutil.cpu_percent(interval=1)
        carga1, carga5, carga15 = carregar_media_carga()

        # Memória principal.
        mem = psutil.virtual_memory()
        uso_memoria = mem.percent
        memoria_total_b = int(mem.total)
        memoria_usada_b = int(mem.used)

        # Memória de swap.
        swap = psutil.swap_memory()
        uso_swap = swap.percent
        swap_total_b = int(swap.total)
        swap_usada_b = int(swap.used)

        # ------- Disco ------- #
        disco = psutil.disk_usage('/')
        uso_disco = disco.percent
        disco_total_b = int(disco.total)
        disco_usado_b = int(disco.used)
        disco_livre_b = int(disco.free)

    
        # Estatísticas de rede acumuladas desde o boot.
        rede = psutil.net_io_counters()
        rede_enviada_b = int(rede.bytes_sent)
        rede_recebida_b = int(rede.bytes_recv)

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


        # Frequência atual da CPU e temperatura aproximada.
        freq_cpu = psutil.cpu_freq()
        freq_cpu_mhz = float(freq_cpu.current) if freq_cpu else 0.0
        temp_cpu_c = obter_temperatura_cpu_c()

        # Uptime do sistema.
        tempo_boot = psutil.boot_time()
        uptime_segundos = int(time.time() - tempo_boot)

        # ----------------------------- Métricas de processos ----------------------------- #
        # A leitura pode falhar caso o processo termine durante a coleta; Ignorei esses casos.
        for processo in psutil.process_iter([
            'name', 'username', 'pid', 'memory_percent', 'num_threads', 'create_time', 'status'
        ]):
            try:
                dados = processo.info

                # Conversão do timestamp de criação do processo para string legível para as análises.
                data_inicio_humana = (
                    datetime.fromtimestamp(dados.get('create_time')).strftime("%Y-%m-%d %H:%M:%S")
                    if dados.get('create_time') else ""
                )

                # Oneshot ativa um cache temporário dentro do with, fazendo com que a leitura fique mais rapido e consuma menos recursos.
                with processo.oneshot():
                    rss_bytes = int(processo.memory_info().rss) # Qtd de RAM que o processo ocupa
                    # Amostragem curta para CPU por processo.
                    cpu_proc_percent = processo.cpu_percent(interval=0.1)
                    # Contadores de I/O quando disponíveis.
                    io = processo.io_counters() if processo.is_running() else None
                    leitura_bytes = int(io.read_bytes) if io else 0
                    escrita_bytes = int(io.write_bytes) if io else 0

                linhas.append({
                    "Nome da Maquina": nome_maquina,
                    "Data da Coleta": data_coleta,

                    'Uso de CPU': uso_cpu,
                    'Load1': carga1,
                    'Load5': carga5,
                    'Load15': carga15,

                    'Uso de RAM': uso_memoria,
                    'RAM total (bytes)': memoria_total_b,
                    'RAM usada (bytes)': memoria_usada_b,

                    'Uso de Swap': uso_swap,
                    'Swap total (bytes)': swap_total_b,
                    'Swap usada (bytes)': swap_usada_b,

                    'Uso de Disco': uso_disco,
                    'Disco total (bytes)': disco_total_b,
                    'Disco usado (bytes)': round(disco_usado_b, 2),
                    'Disco livre (bytes)': round(disco_livre_b, 2),
                    'Net bytes enviados': rede_enviada_b,
                    'Net bytes recebidos': rede_recebida_b,
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

                    'Freq CPU (MHz)': freq_cpu_mhz,
                    'Temp CPU (C)': temp_cpu_c if temp_cpu_c is not None else "", # condição que, dependendo da configuração, não consegue capturar temperatura

                    'Uptime (s)': uptime_segundos,

                    "Processo": dados.get('name'),
                    "PID": dados.get('pid'),
                    "Usuario": dados.get('username'),
                    'CPU proc (%)': cpu_proc_percent,
                    'MEM proc (%)': dados.get('memory_percent'),
                    'Threads': dados.get('num_threads'),
                    'RSS (bytes)': rss_bytes,
                    'IO Leitura (bytes)': leitura_bytes,
                    'IO Escrita (bytes)': escrita_bytes,
                    "Quando foi iniciado": data_inicio_humana,
                    "Status": dados.get('status')
                })

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # Pula um processo se não conseguir ler ou não tiver permissão.
                continue

        df = pd.DataFrame(linhas)
        if os.path.exists(CSV_PATH):
            df.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
        else:
            df.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)

        s3.upload_file(arquivo_local, bucket, destino_s3)
        print("✅ Upload concluído com sucesso!")
        time.sleep(INTERVALO_SEGUNDOS)

except KeyboardInterrupt:
    print("Monitoramento finalizado.")