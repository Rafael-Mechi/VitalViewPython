import psutil
import pandas as pd
import os
import boto3
import time
import socket
from datetime import datetime
from psutil._common import bytes2human
from botocore.exceptions import BotoCoreError, ClientError

# --- Configurações ---
DATA_PATH = "DadosRecebidos"
CSV_PATH = "DadosRecebidos/captura_secundaria.csv"
os.makedirs(DATA_PATH, exist_ok=True)

S3_ENABLE = True
S3_BUCKET = "bucket-raw-2025-11-10-11512"
S3_OBJECT = f"captura_secundaria.csv"

s3 = boto3.client("s3")

def s3_chave_destino():
    timestamp_s3 = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{timestamp_s3}_{S3_OBJECT}"

INTERVALO_SEGUNDOS = 60

try:
    while True:
        linhas_processos = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hostname = socket.gethostname()

        fields = ['name', 'pid', 'memory_percent','cpu_percent','num_threads', 'status']

        for proc in psutil.process_iter(fields):
            try:
                mem_info = proc.info.get('memory_percent', 0.0)
                memoria_formatada = round(mem_info, 1)

                cpu_info = proc.info.get('cpu_percent', 0.0)
                cpu_formatada = round(cpu_info, 1)
                
                create_time_local = datetime.fromtimestamp(proc.info.get('create_time', 0)).strftime("%Y-%m-%d %H:%M:%S")

                linhas_processos.append({
                    "Timestamp_Coleta": timestamp,
                    "Hostname": hostname,
                    "PID": proc.info.get('pid'),
                    "Nome_Processo": proc.info.get('name'),
                    "Status": proc.info.get('status'),
                    "Uso_Ram_Percent": memoria_formatada,
                    "Uso_Cpu_Percent": cpu_formatada,
                    "Num_Threads": proc.info.get('num_threads'),
                })

            except (psutil.NoSuchProcess, psutil.AccessDenied, Exception) as e:
                continue

        # Salva CSV: Criando o DataFrame dos PROCESSOS
        df_processos = pd.DataFrame(linhas_processos)

        # Se houver dados de processos para salvar
        if not df_processos.empty:
            
            # --- Lógica de Salvamento CSV ---
            # Salva o DataFrame de PROCESSOS (df_processos) no arquivo
            if os.path.exists(CSV_PATH):
                # Anexa sem cabeçalho se o arquivo já existir
                df_processos.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
            else:
                # Salva com cabeçalho se o arquivo for novo
                df_processos.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)

            print(f"\n📁 Dados de {len(df_processos)} processos salvos em: {CSV_PATH}")
        else:
            print("\n⚠️ Nenhum processo coletado ou dados inválidos.")
            
        #  # S3
        # if S3_ENABLE and os.path.exists(CSV_PATH): # Só tenta o upload se o arquivo local existir
        #     try:
        #         destino = s3_chave_destino()
        #         s3.upload_file(CSV_PATH, S3_BUCKET, destino, ExtraArgs={"ContentType": "text/csv"})
        #         print(f"✅ S3 upload: s3://{S3_BUCKET}/{destino}")
        #     except (BotoCoreError, ClientError) as e:
        #         print(f"⚠️ Falha no upload S3: {e}")

        # Mensagem de Log
        print(
            f"--- {timestamp} | Host: {hostname} | Total de Processos Coletados: {len(df_processos)} ---"
        )

        time.sleep(INTERVALO_SEGUNDOS)  

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")