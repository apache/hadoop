#!/bin/sh


OUTFILE=/scratch/packets
CAPTUREPID=`cat /var/tmp/packetcapturepid`
TMPPARSED=/scratch/parsedpackets
echo "capture running as process $CAPTUREPID"
sudo kill $CAPTUREPID
rm /var/tmp/packetcapturepid
/usr/sbin/tcpdump -vr $OUTFILE >$TMPPARSED
nc localhost 9093 <<HERE
add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8 TcpDumpV 0 $TMPPARSED 0
close
HERE

