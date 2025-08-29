import psutil
import pandas as pd
import os
import time
from datetime import datetime

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

while True:
    for proc in psutil.process_iter(['name', 'username', 'pid', 'memory_percent']):
        info = proc.info
        timestamp = datetime.now()
        username = info.get('username')  
        name = info.get('name')         
        pid = info.get('pid')
        memoryPercent = info.get('memory_percent') 
        print(f"Timestamp: {timestamp},Usuario: {username}, Processo: {name}, pid: {pid}, RAM usada: {memoryPercent:.4f}")

        dados = pd.DataFrame([{
            "timestamp:": timestamp,
            "username": username,
            "name": name,
            "pid": pid,
            "memory_percent": memoryPercent
        }])
        
        if os.path.exists(DATA_PATH):
            dados.to_csv(CSV_PATH, mode="a", index=False, sep=";", header=False)
        else:
            os.mkdir(DATA_PATH)
            dados.to_csv(CSV_PATH, mode="a", index=False, sep=";", header=False)

    time.sleep(10)
