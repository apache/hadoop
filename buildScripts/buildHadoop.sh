#!/bin/bash

find . -name pom.xml -exec sed -i -e 's/<require.snappy>false<\/require.snappy>/<require.snappy>true<\/require.snappy>/g' {} \;
mvn package -Pdist,native -Dyarn.arg="install --offline" -DskipTests -Drequire.snappy -Dmaven.javadoc.skip=true -U -X 
