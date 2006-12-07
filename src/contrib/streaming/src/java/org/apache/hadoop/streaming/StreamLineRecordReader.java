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

package org.apache.hadoop.streaming;

import java.io.*;
import java.nio.charset.MalformedInputException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * Similar to org.apache.hadoop.mapred.TextRecordReader, 
 * but delimits key and value with a TAB.
 * @author Michel Tourn
 */
public class StreamLineRecordReader extends StreamBaseRecordReader {

  public StreamLineRecordReader(FSDataInputStream in, FileSplit split, Reporter reporter,
      JobConf job, FileSystem fs) throws IOException {
    super(in, split, reporter, job, fs);
    gzipped_ = StreamInputFormat.isGzippedInput(job);
    if (gzipped_) {
      din_ = new BufferedInputStream( (new GZIPInputStream(in_) ) );
    } else {
      din_ = in_;
    }
  }

  public void seekNextRecordBoundary() throws IOException {
    if (gzipped_) {
      // no skipping: use din_ as-is 
      // assumes splitter created only one split per file
      return;
    } else {
      int bytesSkipped = 0;
      if (start_ != 0) {
        in_.seek(start_ - 1);
        // scan to the next newline in the file
        while (in_.getPos() < end_) {
          char c = (char) in_.read();
          bytesSkipped++;
          if (c == '\r' || c == '\n') {
            break;
          }
        }
      }

      //System.out.println("getRecordReader start="+start_ + " end=" + end_ + " bytesSkipped"+bytesSkipped);
    }
  }

  public synchronized boolean next(Writable key, Writable value) throws IOException {
    if (!(key instanceof Text)) {
      throw new IllegalArgumentException("Key should be of type Text but: "
          + key.getClass().getName());
    }
    if (!(value instanceof Text)) {
      throw new IllegalArgumentException("Value should be of type Text but: "
          + value.getClass().getName());
    }

    Text tKey = (Text) key;
    Text tValue = (Text) value;
    byte[] line;

    if ( !gzipped_  ) {
      long pos = in_.getPos();
      if (pos >= end_) return false;
    }
    
    line = UTF8ByteArrayUtils.readLine((InputStream) din_);
    if (line == null) return false;
    int tab = UTF8ByteArrayUtils.findTab(line);
    if (tab == -1) {
      tKey.set(line);
      tValue.set("");
    } else {
      UTF8ByteArrayUtils.splitKeyVal(line, tKey, tValue, tab);
    }
    numRecStats(line, 0, line.length);
    return true;
  }

  boolean gzipped_;
  InputStream din_; // GZIP or plain  
}
