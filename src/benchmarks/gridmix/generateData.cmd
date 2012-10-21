@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.


SET GRID_MIX=%~dp0
CD %GRID_MIX%
CALL "%GRID_MIX%gridmix-env.cmd"


REM Smaller data set is used by default.
set COMPRESSED_DATA_BYTES=2147483648
set UNCOMPRESSED_DATA_BYTES=536870912
set INDIRECT_DATA_BYTES=58720256


REM Number of partitions for output data
if not defined NUM_MAPS (
  set NUM_MAPS=100
)

set INDIRECT_DATA_FILES=200

REM If the env var USE_REAL_DATASET is set, then use the params to generate the bigger (real) dataset.
if defined USE_REAL_DATASET (
  echo "Using real dataset"
  REM 2TB data compressing to approx 500GB
  set COMPRESSED_DATA_BYTES=2147483648000
  REM 500GB
  set UNCOMPRESSED_DATA_BYTES=536870912000
  REM Default approx 70MB per data file, compressed
  set INDIRECT_DATA_BYTES=58720256000 
)


SET /a COMPRESSED_BYTES_PER_MAP=%COMPRESSED_DATA_BYTES% / %NUM_MAPS%
SET /a UNCOMPRESSED_BYTES_PER_MAP=%UNCOMPRESSED_DATA_BYTES% / %NUM_MAPS%
SET /a INDIRECT_BYTES_PER_MAP=%INDIRECT_DATA_BYTES% / %NUM_MAPS%

CALL "%HADOOP_HOME%/bin/hadoop" jar ^
  %EXAMPLE_JAR% randomtextwriter ^
  -D "test.randomtextwrite.total_bytes=%COMPRESSED_DATA_BYTES%" ^
  -D "test.randomtextwrite.bytes_per_map=%COMPRESSED_BYTES_PER_MAP%" ^
  -D "test.randomtextwrite.min_words_key=5" ^
  -D "test.randomtextwrite.max_words_key=10" ^
  -D "test.randomtextwrite.min_words_value=100" ^
  -D "test.randomtextwrite.max_words_value=10000" ^
  -D "mapred.output.compress=true" ^
  -D "mapred.map.output.compression.type=BLOCK" ^
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ^
  %VARCOMPSEQ%
  
CALL "%HADOOP_HOME%/bin/hadoop" jar ^
  %EXAMPLE_JAR% randomtextwriter ^
  -D "test.randomtextwrite.total_bytes=%COMPRESSED_DATA_BYTES%" ^
  -D "test.randomtextwrite.bytes_per_map=%COMPRESSED_BYTES_PER_MAP%" ^
  -D "test.randomtextwrite.min_words_key=5" ^
  -D "test.randomtextwrite.max_words_key=5" ^
  -D "test.randomtextwrite.min_words_value=100" ^
  -D "test.randomtextwrite.max_words_value=100" ^
  -D "mapred.output.compress=true" ^
  -D "mapred.map.output.compression.type=BLOCK" ^
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ^
  %FIXCOMPSEQ%

CALL "%HADOOP_HOME%\bin\hadoop" jar ^
  %EXAMPLE_JAR% randomtextwriter ^
  -D "test.randomtextwrite.total_bytes=%UNCOMPRESSED_DATA_BYTES%" ^
  -D "test.randomtextwrite.bytes_per_map=%UNCOMPRESSED_BYTES_PER_MAP%" ^
  -D "test.randomtextwrite.min_words_key=1" ^
  -D "test.randomtextwrite.max_words_key=10" ^
  -D "test.randomtextwrite.min_words_value=0" ^
  -D "test.randomtextwrite.max_words_value=200" ^
  -D "mapred.output.compress=false" ^
  -outFormat org.apache.hadoop.mapred.TextOutputFormat ^
  %VARINFLTEXT%


CALL "%HADOOP_HOME%\bin\hadoop" jar ^
  %EXAMPLE_JAR% randomtextwriter ^
  -D "test.randomtextwrite.total_bytes=%INDIRECT_DATA_BYTES%" ^
  -D "test.randomtextwrite.bytes_per_map=%INDIRECT_BYTES_PER_MAP%" ^
  -D "test.randomtextwrite.min_words_key=5" ^
  -D "test.randomtextwrite.max_words_key=5" ^
  -D "test.randomtextwrite.min_words_value=20" ^
  -D "test.randomtextwrite.max_words_value=20" ^
  -D "mapred.output.compress=true" ^
  -D "mapred.map.output.compression.type=BLOCK" ^
  -outFormat org.apache.hadoop.mapred.TextOutputFormat ^
  %FIXCOMPTEXT%

