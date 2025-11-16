import pandas as pd
import os
import time
import psutil
from datetime import datetime
import boto3
import traceback

bucket = "s3-bucket-raw-11102025"
s3 = boto3.client('s3')

dir = '/home/meki/imagensTeste'

while True:

    agora = datetime.now()
    linhas = []

    disco = psutil.disk_usage('/')
    usoDisco = round(disco.used / 1073741824, 2)
    totalDisco = round(disco.total / 1073741824, 2)

    lst = os.listdir(dir)
    numeroArquivos = len(lst)

    tamanhos = []
    for f in lst:
        caminho_arquivo = os.path.join(dir, f)
        tamanho_arquivo = os.path.getsize(caminho_arquivo)
        tamanhos.append(tamanho_arquivo)
    
    media = round(sum(tamanhos) / len(tamanhos) / 1048576, 2)

    paraGb = round(sum(tamanhos) / 1073741824, 2)

    for arq in lst:
        caminho = os.path.join(dir, arq)

        info = os.stat(caminho)
        dataMod = datetime.fromtimestamp(info.st_mtime)
        somenteData = dataMod.date()
        diferenca = (agora - dataMod).total_seconds()
        anos = round(diferenca / (60 * 60 * 24 * 365), 2)

        dados = {
            "total_de_imagens": numeroArquivos,
            "total_de_disco": totalDisco,
            "uso_de_disco_da_imagem": paraGb,
            "uso_de_disco_do_sistema": usoDisco,
            "tamanho_medio_dos_arquivos": media,
            "nome_arquivo": arq,
            "tamanho": round(os.path.getsize(caminho) / 1048576, 2),
            "data_geracao": somenteData,
            "anos_no_sistema": anos
        }

        linhas.append(dados)

    df = pd.DataFrame(linhas)

    caminho_csv = '/home/meki/VitalViewPython/1_srv1_hsl.csv'
    df.to_csv(caminho_csv, mode="w", sep=";", encoding="utf-8", index=False, header=True)

    try:
        nomeCaptura = "1_srv1_hsl.csv"
        s3.upload_file(caminho_csv, bucket, nomeCaptura)
        print("✅ Enviado para o bucket")
    except:
        print(traceback.format_exc())

    time.sleep(3)