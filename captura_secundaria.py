import psutil
import pandas as pd
import os
import boto3
import time
import socket
from datetime import datetime
from psutil._common import bytes2human
from botocore.exceptions import BotoCoreError, ClientError

DATA_PATH = "DadosRecebidos"
CSV_PATH = "DadosRecebidos/captura_secundaria.csv"
os.makedirs(DATA_PATH, exist_ok=True)

S3_ENABLE = True
S3_BUCKET = "bucket-raw-2025-10-23-9773"
S3_OBJECT = f"captura_secundaria.csv"
s3 = boto3.client("s3")

def s3_chave_destino():
    return f"{S3_OBJECT}"

INTERVALO_SEGUNDOS = 2


try:
    while True:

        linhas = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hostname = socket.gethostname()


        # Coleta por processo
        for proc in psutil.process_iter(['name', 'username', 'pid', 'memory_percent', 'num_threads', 'create_time','status']):
            try:

                linhas.append({
                    
                })

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Salva CSV
        df = pd.DataFrame(linhas)
        # sem processos) 
        linha = {


            
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
            
        )

        time.sleep(20)  

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")
