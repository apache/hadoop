#!/usr/bin/gawk

#A small awk script to take normal job history logs and chukwafy
#In particular, they get prefixed with something of the form
#"2008-07-28 23:30:38,865 INFO org.apache.hadoop.chukwa.ChukwaJobHistory: 

BEGIN {
OFS = ""
}

{  # for each record
  #get Timestamp
for(i =1; i <= NF; ++i) {
	if( $i ~ /_TIME=/) {
		split($i, halves, "=");
		ts_msec = substr(halves[2], 2 , length(halves[2]) - 2)
		break;
	}
}
if(ts_msec == 0)
   print "WARNING:  no timestamp in line " > /dev/stderr

print strftime("%Y-%m-%d %H:%M:%S", ts_msec/1000)  , "," , (ts_msec%1000) , " INFO org.apache.hadoop.chukwa.ChukwaJobHistory: " , $0
}