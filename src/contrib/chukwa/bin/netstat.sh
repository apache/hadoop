#!/bin/bash
netstat -a | grep ESTABLISH | grep -v '        0      0'
