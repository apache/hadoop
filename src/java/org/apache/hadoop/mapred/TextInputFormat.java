/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.*;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class TextInputFormat extends InputFormatBase implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;
  
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }
  
  protected boolean isSplitable(FileSystem fs, Path file) {
    return compressionCodecs.getCodec(file) == null;
  }
  
  public RecordReader getRecordReader(InputSplit genericSplit,
                                      JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(genericSplit.toString());
    FileSplit split = (FileSplit) genericSplit;
    long start = split.getStart();
    long end = start + split.getLength();
    final Path file = split.getPath();
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = FileSystem.get(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    InputStream in = fileIn;
    if (codec != null) {
      in = codec.createInputStream(fileIn);
      end = Long.MAX_VALUE;
    } else if (start != 0) {
      fileIn.seek(start-1);
      LineRecordReader.readLine(fileIn, null);
      start = fileIn.getPos();
    }
    
    return new LineRecordReader(in, start, end);
  }
}

