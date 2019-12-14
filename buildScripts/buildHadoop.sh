#!/bin/bash

#sed -i "s/<require.snappy>false<\/require.snappy>/<require.snappy>true<\/require.snappy>/g" */*/pom.xml
mvn package -Pdist,native -Dyarn.arg="install --offline" -DskipTests -Drequire.snappy -Dmaven.javadoc.skip=true -U -X 
