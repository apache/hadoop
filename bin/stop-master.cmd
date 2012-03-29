@echo off

:StopHadoop
Taskkill /FI "WINDOWTITLE eq Apache Hadoop Distribution - hadoop   jobtracker"
Taskkill /FI "WINDOWTITLE eq Apache Hadoop Distribution - hadoop   namenode"