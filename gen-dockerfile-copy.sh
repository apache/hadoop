#!/bin/bash
echo COPY .git /build/.git
find . -name 'hadoop-*' -type d -d 1 -exec sh -c 'echo COPY ${0#"./"} /build/${0#"./"}' {} \;
echo COPY dev-support /build/dev-support
echo COPY pom.xml /build/pom.xml
echo COPY LICENSE.txt /build/LICENSE.txt
echo COPY BUILDING.txt /build/BUILDING.txt
echo COPY NOTICE.txt /build/NOTICE.txt
echo COPY README.txt /build/README.txt
