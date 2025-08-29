from datetime import datetime
import psutil as ps
import pandas as pd
import time
import os
from psutil._common import bytes2human

DATA_PATH = '../data'
CSV_PATH = '../data/machine_data.csv'

while True:
    cpu_percent = ps.cpu_percent(interval=0.1, percpu=False)

    mem = ps.virtual_memory()
    mem_percent = mem.percent
    mem_avl = bytes2human(mem.available)
    
    disk = ps.disk_usage('C:\\')
    disk_percent = disk.percent
    disk_avl = bytes2human(disk.free)

    timestamp = datetime.now()

    new_row = pd.DataFrame({
        'timestamp': [timestamp],
        'cpu_percent': [cpu_percent],
        'memory_percent': [mem_percent],
        'memory_available': [mem_avl],
        'disk_percent': [disk_percent],
        'disk_avl': [disk_avl]
    })

    print(new_row)

    if os.path.exists(DATA_PATH):
        new_row.to_csv(CSV_PATH, mode="a", sep=';', encoding='utf-8', index=False, header=False)
    else:
        os.mkdir(DATA_PATH)
        new_row.to_csv(CSV_PATH, mode="a", sep=';', encoding='utf-8', index=False, header=False)



        
    time.sleep(1)
