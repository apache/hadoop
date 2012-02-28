#!/bin/sh
ps -ef | grep apacheds | grep -v grep | cut -f4 -d ' ' |xargs kill -9

