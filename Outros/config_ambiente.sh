#!/bin/bash

sudo apt update && sudo apt upgrade -y

sudo apt install python3-pip -y

sudo apt install python3-venv -y

python3 -m venv venv

source venv/bin/activate

pip install -r requirements.txt 

#sudo apt install python3-psutil

#sudo apt install python3-pandas
