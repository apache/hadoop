#!/bin/sh

OUTFILE=/scratch/packets
rm -f $OUTFILE
sudo tcpdump -w $OUTFILE &
CAPTUREPID=$!
echo $CAPTUREPID > /var/tmp/packetcapturepid
chmod 644 /var/tmp/packetcapturepid #to avoid stop being a huge security hole
echo "capture process has PID $CAPTUREPID"
