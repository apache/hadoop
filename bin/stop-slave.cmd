@echo off

:StopHadoop
Taskkill /FI "WINDOWTITLE eq Apache Hadoop Distribution - hadoop   tasktracker"
Taskkill /FI "WINDOWTITLE eq Apache Hadoop Distribution - hadoop   datanode"