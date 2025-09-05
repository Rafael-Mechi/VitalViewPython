from datetime import datetime
import psutil as ps
import pandas as pd
import time
import os
from psutil._common import bytes2human

DATA_PATH = '../data'
CSV_PATH = '../data/machine_data.csv'

os.makedirs(DATA_PATH, exist_ok=True) # Se nao existir, ele cria 

while True:
    cpu_percent = ps.cpu_percent(interval=0.1, percpu=False)

    mem = ps.virtual_memory()
    mem_percent = mem.percent
    mem_avl = bytes2human(mem.available)
    
    disk = ps.disk_usage('/')
    disk_percent = disk.percent
    disk_avl = bytes2human(disk.free)

    timestamp = datetime.now()
    data_hora_formatada = timestamp.strftime("%d/%m/%y - %H:%M:%S")
    new_row = pd.DataFrame({
        'timestamp': [data_hora_formatada],
        'cpu_percent': [cpu_percent],
        'memory_percent': [mem_percent],
        'memory_available': [mem_avl],
        'disk_percent': [disk_percent],
        'disk_avl': [disk_avl]
    })

    print(new_row)

    #if os.path.exists(DATA_PATH):
     #   new_row.to_csv(CSV_PATH, mode="a", sep=';', encoding='utf-8', index=False, header=False)
    #else:
     #   os.mkdir(DATA_PATH)
    #    new_row.to_csv(CSV_PATH, mode="a", sep=';', encoding='utf-8', index=False, header=False)

    if os.path.exists(CSV_PATH):
        # próximas vezes → só adiciona sem header
        
        new_row.to_csv(CSV_PATH, mode="a", sep=";", encoding="utf-8", index=False, header=False)
    else:
        # primeira vez → cria com header
        new_row.to_csv(CSV_PATH, mode="w", sep=";", encoding="utf-8", index=False, header=True)



        
    time.sleep(10)
