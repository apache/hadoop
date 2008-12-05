#!/bin/sh

CAPTURETIME=30 #seconds
OUTFILE=/scratch/packets
echo "capturing $CAPTURETIME seconds of traffic"
sudo tcpdump -w $OUTFILE &
sleep $CAPTURETIME
kill $!
echo -n "done capturing; output size in bytes: "
wc -c $OUTFILE | grep -o '[0-9]*'
sleep 2
/usr/sbin/tcpdump -r $OUTFILE | wc -c
#do chukwa collection here

echo ""
