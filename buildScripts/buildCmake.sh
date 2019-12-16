#!/bin/bash

tar -xvf cmake-3.13.3-Linux-x86_64.tar.gz 
cd cmake-3.13.3-Linux-x86_64/
echo "Creating cmake Symlink"
sudo ln -s  `pwd`/cmake-3.13.3-Linux-x86_64/bin/cmake /usr/bin/cmake
