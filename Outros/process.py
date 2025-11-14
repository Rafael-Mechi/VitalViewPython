import psutil
import pandas as pd
import os
import time
import socket
from datetime import datetime
from psutil._common import bytes2human

# Obs:
# O valor de "Uso de CPU" retornado por psutil.cpu_percent() representa o uso total de todos os núcleos da CPU.
# Ou seja, em máquinas com múltiplos núcleos, o valor pode ultrapassar 100%.
# Exemplo: em um sistema com 8 núcleos, o valor máximo possível é 800%.

DATA_PATH = "../data"
CSV_PATH = "../data/process_data.csv"

wtrmark = """
__     ___ _        _  __     ___               
\ \   / (_) |_ __ _| | \ \   / (_) _____      __
 \ \ / /| | __/ _` | |  \ \ / /| |/ _ \ \ /\ / /
  \ V / | | || (_| | |   \ V / | |  __/\ V  V / 
   \_/  |_|\__\__,_|_|    \_/  |_|\___| \_/\_/  
"""

print(wtrmark)

try:
    while True:

        linhas = []

        for proc in psutil.process_iter(['name', 'username', 'pid', 'memory_percent', 'num_threads', 'create_time','status']):
            try:
                info = proc.info
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                create_time_human = datetime.fromtimestamp(info.get('create_time')).strftime("%Y-%m-%d %H:%M:%S") \
                                    if info.get('create_time') else ""
                memory_human = bytes2human(proc.memory_info().rss)
                cpu_usage = proc.cpu_percent(interval=0.1)
                hostname = socket.gethostname()

                linhas.append({
                    "Nome da Maquina": hostname,
                    "Data da Coleta": timestamp,
                    "Usuario": info.get('username'),
                    "Processo": info.get('name'),
                    "ID": info.get('pid'),
                    "Uso de Ram": memory_human,
                    "Uso de CPU":  round(cpu_usage,2),
                    "Uso de Threads": info.get('num_threads'),
                    "Quando foi iniciado": create_time_human,
                    "Status": info.get('status')
                })
                
                print(f"Timestamp: {timestamp},Usuario: {info.get('username')}, Processo: {info.get('name')}, pid: {info.get('pid')}, RAM usada: {memory_human}, CPU usada no proceso: { round(cpu_usage,2)},Numero de threads: {info.get('num_threads')}, Horario que processo iniciou: {create_time_human}, Status: {info.get('status')} ")

                dados = pd.DataFrame(linhas)

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if os.path.exists(DATA_PATH):
            dados.to_csv(CSV_PATH, mode="a", index=False, sep=";", header=False)
        else:
            os.mkdir(DATA_PATH)
            dados.to_csv(CSV_PATH, mode="w", index=False, sep=";", header=True)

        time.sleep(10)

except KeyboardInterrupt:
    print("\nMonitoramento finalizado.")

    

