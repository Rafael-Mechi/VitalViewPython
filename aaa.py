import platform
import os

sistemaOperacional = platform.system()
usuario = os.getlogin()

if sistemaOperacional == 'Linux':
    dir = f"/home/{usuario}/imagensTeste"
else:
    dir = f"C:/Users/{usuario}/Documentos"

print(dir)