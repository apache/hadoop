#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

GRID_DIR=`dirname "$0"`
GRID_DIR=`cd "$GRID_DIR"; pwd`
source $GRID_DIR/gridmix-env

# Smaller data set is used by default.
COMPRESSED_DATA_BYTES=2147483648
UNCOMPRESSED_DATA_BYTES=536870912
INDIRECT_DATA_BYTES=58720256

# Number of partitions for output data
if [ -z ${NUM_MAPS} ] ; then
  NUM_MAPS=100
fi

INDIRECT_DATA_FILES=200

# If the env var USE_REAL_DATASET is set, then use the params to generate the bigger (real) dataset.
if [ ! -z ${USE_REAL_DATASET} ] ; then
  echo "Using real dataset"
  # 2TB data compressing to approx 500GB
  COMPRESSED_DATA_BYTES=2147483648000
  # 500GB
  UNCOMPRESSED_DATA_BYTES=536870912000
  # Default approx 70MB per data file, compressed
  INDIRECT_DATA_BYTES=58720256000 
fi

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D mapreduce.randomtextwriter.totalbytes=${COMPRESSED_DATA_BYTES} \
  -D mapreduce.randomtextwriter.bytespermap=$((${COMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D mapreduce.randomtextwriter.minwordskey=5 \
  -D mapreduce.randomtextwriter.maxwordskey=10 \
  -D mapreduce.randomtextwriter.minwordsvalue=100 \
  -D mapreduce.randomtextwriter.maxwordsvalue=10000 \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat \
  ${VARCOMPSEQ} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D mapreduce.randomtextwriter.totalbytes=${COMPRESSED_DATA_BYTES} \
  -D mapreduce.randomtextwriter.bytespermap=$((${COMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D mapreduce.randomtextwriter.minwordskey=5 \
  -D mapreduce.randomtextwriter.maxwordskey=5 \
  -D mapreduce.randomtextwriter.minwordsvalue=100 \
  -D mapreduce.randomtextwriter.maxwordsvalue=100 \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat \
  ${FIXCOMPSEQ} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D mapreduce.randomtextwriter.totalbytes=${UNCOMPRESSED_DATA_BYTES} \
  -D mapreduce.randomtextwriter.bytespermap=$((${UNCOMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D mapreduce.randomtextwriter.minwordskey=1 \
  -D mapreduce.randomtextwriter.maxwordskey=10 \
  -D mapreduce.randomtextwriter.minwordsvalue=0 \
  -D mapreduce.randomtextwriter.maxwordsvalue=200 \
  -D mapreduce.output.fileoutputformat.compress=false \
  -outFormat org.apache.hadoop.mapreduce.lib.output.TextOutputFormat \
  ${VARINFLTEXT} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D mapreduce.randomtextwriter.totalbytes=${INDIRECT_DATA_BYTES} \
  -D mapreduce.randomtextwriter.bytespermap=$((${INDIRECT_DATA_BYTES} / ${INDIRECT_DATA_FILES})) \
  -D mapreduce.randomtextwriter.minwordskey=5 \
  -D mapreduce.randomtextwriter.maxwordskey=5 \
  -D mapreduce.randomtextwriter.minwordsvalue=20 \
  -D mapreduce.randomtextwriter.maxwordsvalue=20 \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapreduce.lib.output.TextOutputFormat \
  ${FIXCOMPTEXT} &
