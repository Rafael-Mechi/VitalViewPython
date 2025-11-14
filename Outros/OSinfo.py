import platform
import os
import pandas as pd

system_info = platform.uname()

DATA_PATH = "../data"
CSV_PATH = "../data/dados_OS.csv"

wtrmark = """
__     ___ _        _  __     ___               
\ \   / (_) |_ __ _| | \ \   / (_) _____      __
 \ \ / /| | __/ _` | |  \ \ / /| |/ _ \ \ /\ / /
  \ V / | | || (_| | |   \ V / | |  __/\ V  V / 
   \_/  |_|\__\__,_|_|    \_/  |_|\___| \_/\_/  
"""

print(wtrmark)

print(f"OS: {system_info.system}")
print(f"User & Device: {system_info.node}")
print(f"Version: {system_info.version}")
print(f"Machine: {system_info.machine}")

dados = pd.DataFrame([{
        "OS": system_info.system,
        "User & Device": system_info.node,
        "Version": system_info.version,
        "Machine": system_info.machine
    }])

if os.path.exists(DATA_PATH):
    dados.to_csv(CSV_PATH, sep=';', encoding='utf-8', index=False)
else:
    os.mkdir(DATA_PATH)
    dados.to_csv(CSV_PATH, sep=';', encoding='utf-8', index=False)
