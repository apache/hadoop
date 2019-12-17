#!/bin/bash

mkdir pushHadoopTar/HadoopSourceBinary
cp -rf hadoop-dist/target/hadoop-3.2.1.tar.gz pushHadoopTar/HadoopSourceBinary/
ls

cd pushHadoopTar
ls

mkdir -p HadoopSourceBinary
mkdir -p deb/tmp/
cp -rf HadoopSourceBinary deb/tmp/
cp -rf DEBIAN deb/

sh pushHadoopTar/make-flipkart-hadoop-source-tar-3.2.1-deb
dpkg -b deb flipkart-hadoop-source-tar-repo.deb
reposervice --host 10.24.0.41 --port "8080" pubrepo --repo  fk-hadoop-source-tar-repo  --appkey dummy --debs flipkart-hadoop-source-tar-repo.deb
cd $cwd
