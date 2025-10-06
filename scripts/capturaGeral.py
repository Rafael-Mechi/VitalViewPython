import psutil
import pandas as pd
import os
import time
import socket
from datetime import datetime
from psutil._common import bytes2human

#from slack_sdk import WebClient
#from slack_sdk.errors import SlackApiError

#client = WebClient(token="Insira o Token aqui")

DATA_PATH = "data"
CSV_PATH = "data/process_data.csv"
os.makedirs(DATA_PATH, exist_ok=True)

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
                    "Status": info.get('status')
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Salva CSV
        df = pd.DataFrame(linhas)
        if os.path.exists(CSV_PATH):
            df.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
        else:
            df.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)

        # --- ALERTA SLACK ---
       # if cpu_percent > 25 or mem_percent > 25 or disk_percent > 25:
        #    alerta = (
         #       f"⚠️ *Alerta de uso elevado detectado!*\n"
          #      f"🕒 {timestamp}\n"
           #     f"👤 Servidor: {hostname}\n"
            #    f"💻 CPU: {cpu_percent}%\n"
             #   f"🧠 RAM: {mem_percent}%\n"
              #  f"💾 Disco: {disk_percent}%"
            #)
            #try:
             #   client.chat_postMessage(channel="#suporte-slack", text=alerta)
              #  print("Alerta enviado para o Slack.")
            #except SlackApiError as e:
             #   print("Erro ao enviar alerta:", e.response["error"])

        #time.sleep(30)  # evita flood de alertas

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")