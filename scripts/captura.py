from datetime import datetime
import psutil as ps
import pandas as pd
import time
from psutil._common import bytes2human

file = '../data/machine_data.csv'

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

    try:
        df = pd.read_csv(file, sep=';')
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(file, sep=';', encoding='utf-8', index=False)
    except:
        new_row.to_csv(file, sep=';', encoding='utf-8', index=False)


    time.sleep(10)
