import pandas as pd
import os
import time
import psutil
from datetime import datetime

dir = '/home/meki/imagensTeste'
nomes = []
tamanhos = []
dataGeracao = []
tempoNoSistema = []

while True:
    linhas = []

    agora = datetime.now()

    tamanhoTotal = 0
    for arq in os.listdir(dir):
        disco = psutil.disk_usage('/')
        usoDisco = round(disco.used / 1073741824, 2)
        totalDisco = round(disco.total / 1073741824, 2)

        lst = os.listdir(dir)
        numeroArquivos = len(lst)
        media = round(os.path.getsize(dir) / len(lst) / 1048576, 2)

        tamDir = os.path.getsize(dir)
        paraGb = round((tamDir / 1073741824), 2)
        
        caminho = os.path.join(dir, arq)

        tamanhoTotal += os.path.getsize(caminho)

        # propriedades do arquivo
        nome = os.path.basename(caminho)
        tamanho = round(os.path.getsize(caminho) / 1048576, 2)

        info = os.stat(caminho)
        dataMod = datetime.fromtimestamp(info.st_mtime)
        somenteData = dataMod.date()

        diferenca = (agora - dataMod).total_seconds()

        tempoNoSistema = round(diferenca / (60 * 60 * 24 * 365), 2) #tempo em anos

        dados = {
            "total_de_imagens": numeroArquivos,
            "total_de_disco": totalDisco,
            "uso_de_disco_da_imagem": paraGb,
            "uso_de_disco_do_sistema": usoDisco,
            "tamanho_medio_dos_arquivos": media,
            "nome_arquivo": nome,
            "tamanho": tamanho,
            "data_geracao": somenteData,
            "anos_no_sistema": tempoNoSistema
        }

        linhas.append(dados)

        df = pd.DataFrame(linhas)
        
        df.to_csv('/home/meki/VitalViewPython/c.csv', mode="w", sep=";", encoding="utf-8", index=False, header=True)

    time.sleep(3)

print(numeroArquivos)
print(nome)
print(tamanhoImagem)
print(dataMod)