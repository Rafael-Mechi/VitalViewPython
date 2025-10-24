import requests
import os
import time
from datetime import datetime

while True:
    nomeImagem = f"imagem_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    caminho = "/home/meki/imagensTeste/"
    url = "https://picsum.photos/1024"

    img = requests.get(url)

    # garante que o diretório existe
    os.makedirs(caminho, exist_ok=True)

    # junta caminho + nome do arquivo
    with open(os.path.join(caminho, nomeImagem), "wb") as f:
        f.write(img.content)

    print(f"[{datetime.now()}] Imagem salva: {nomeImagem}")
    time.sleep(5)
