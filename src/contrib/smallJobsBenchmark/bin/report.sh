#!/bin/bash

echo "DataLines, Maps, Reduces, AvgTime " > logs/report.txt
for logFile in `ls logs/*.log`
do
#       tail -n $((${TIMES}+5))  ${logFile} >> logs/report.txt
        tail -n 1  ${logFile} >> logs/report.txt
done

cat  logs/report.txt
