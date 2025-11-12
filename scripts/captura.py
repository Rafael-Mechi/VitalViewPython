import psutil
import pandas as pd
import os
import time
import socket
from datetime import datetime
import boto3


# ------------------------ Configuração básica ------------------------ #
CAMINHO_DADOS = "data"
CAMINHO_CSV = "data/process_data.csv"
INTERVALO_SEGUNDOS = 2
os.makedirs(CAMINHO_DADOS, exist_ok=True)
s3 = boto3.client('s3')


bucket = "bucket-raw-2025-10-23-9773"
arquivo_local = "data/process_data.csv"
destino_s3 = "process_data.csv"


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


# Lógica para calcular a taxa de transferência (leitura e escrita de disco)
# 1) Pegar o snapshot acumulado anterior (contadores desde o boot)
io_anterior = psutil.disk_io_counters()


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

        # --- Disco --- #
        disco = psutil.disk_usage('/')
        uso_disco = disco.percent
        disco_total_b = int(disco.total)
        disco_usado_b = int(disco.used)
        disco_livre_b = int(disco.free)

        io_atual = psutil.disk_io_counters() #captura atual

        # Taxa de transferência
        leitura_mb = float(io_atual.read_bytes - io_anterior.read_bytes) / (1024 * 1024)
        escrita_mb = float(io_atual.write_bytes - io_anterior.write_bytes) / (1024 * 1024)
        taxa_leitura = leitura_mb / INTERVALO_SEGUNDOS
        taxa_escrita = escrita_mb / INTERVALO_SEGUNDOS

        # Latência média 
        total_leitura = io_atual.read_count - io_anterior.read_count
        tempo_leitura = io_atual.read_time - io_anterior.read_time
        total_escrita = io_atual.write_count - io_anterior.write_count
        tempo_escrita = io_atual.write_time - io_anterior.write_time

        latencia_leitura = (tempo_leitura / total_leitura) if total_leitura > 0 else 0
        latencia_escrita = (tempo_escrita / total_escrita) if total_escrita > 0 else 0

        # Atualiza a captura anterior como a atual
        io_anterior = io_atual


        


        # Estatísticas de rede acumuladas desde o boot.
        rede = psutil.net_io_counters()
        rede_enviada_b = int(rede.bytes_sent)
        rede_recebida_b = int(rede.bytes_recv)

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
                    'Taxa leitura (MB)': taxa_leitura,
                    'Taxa Escrita (MB)': taxa_escrita,
                    "Latência leitura (ms)": latencia_leitura,
                    'Latência escrita (ms)': latencia_escrita,

                    'Net bytes enviados': rede_enviada_b,
                    'Net bytes recebidos': rede_recebida_b,

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
        if os.path.exists(CAMINHO_CSV):
            df.to_csv(CAMINHO_CSV, mode="a", sep=";", encoding="utf-8", index=False, header=False)
        else:
            df.to_csv(CAMINHO_CSV, mode="w", sep=";", encoding="utf-8", index=False, header=True)

        s3.upload_file(arquivo_local, bucket, destino_s3)
        print("✅ Upload concluído com sucesso!")
        time.sleep(INTERVALO_SEGUNDOS)

except KeyboardInterrupt:
    print("Monitoramento finalizado.")