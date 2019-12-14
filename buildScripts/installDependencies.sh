#!/bin/bash

sudo grep -r security.debian.org /etc/apt/
sudo sed -i '/security.debian.org/s/^/#/'  /etc/apt/sources.list 
sudo apt-get update
sudo apt-get install protobuf-compiler liblzo2-2 libc6 --allow-unauthenticated -y
sudo apt-get install pkg-config  --yes --allow-unauthenticated
sudo apt-get install --yes --allow-unauthenticated build-essential
sudo apt-get install --yes --allow-unauthenticated  make  libsnappy-dev bzip2 libbz2-dev libjansson-dev zlib1g-dev libncurses5-dev libssl-dev
