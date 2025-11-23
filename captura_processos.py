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
# ------------------------------------------------------------ #
# 🚨 ATENÇÃO: ANTES DE EXECUTAR, CONFIGURE O nome_captura!    #
# ------------------------------------------------------------ #
#Ele deve seguir o formato:                                    #
#     id_nomeServidor_nomeHospital   
#                                                              #
# Exemplo:                                                     #
#     12_Server01_HospitalCentral                              #
# ------------------------------------------------------------ #
bucket = "raw-2011"
nome_captura = "1_Servidor1_Siriolibanes"

INTERVALO_SEGUNDOS = 60
PASTA_ARQUIVO = "ProcessosRecebidos"
NOME_CAPTURA = f"processos_{nome_captura}.csv"
LOCALIZACAO_CAPTURA =  f"ProcessosRecebidos/processos_{nome_captura}.csv"
os.makedirs(PASTA_ARQUIVO, exist_ok=True)
s3 = boto3.client("s3")

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

        # Aumenta o número do arquivo antes de criar o nome

        # Atualiza nomes
        NOME_CAPTURA = f"processos_{nome_captura}.csv"
        LOCALIZACAO_CAPTURA = f"{PASTA_ARQUIVO}/{NOME_CAPTURA}"

        # Salva o CSV localmente
        df_processos = pd.DataFrame(linhas_processos)
        df_processos.to_csv(LOCALIZACAO_CAPTURA, mode="w", sep=";", encoding="utf-8", index=False)

        try:
            # Envia para o bucket
            s3.upload_file(LOCALIZACAO_CAPTURA, bucket, NOME_CAPTURA)
            print("✅ Enviado para o bucket")
        except:
            print("(Atenção) Arrume as credenciais da AWS!")
            break


        # Mensagem de Log
        print(
            f"---Total de Processos Coletados: {len(df_processos)} ---"
        )

        time.sleep(INTERVALO_SEGUNDOS)  

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")