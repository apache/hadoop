#!/bin/bash
cd protobuf-2.5.0
sudo ./configure
sudo make clean
sudo make
sudo make install
sudo ldconfig
protoc --version
cd $cwd

