/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.zip.GZIPInputStream; 

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Similar to org.apache.hadoop.mapred.TextRecordReader, 
 * but delimits key and value with a TAB.
 * @author Michel Tourn
 */
public class StreamLineRecordReader extends StreamBaseRecordReader 
{

  public StreamLineRecordReader(
    FSDataInputStream in, FileSplit split, Reporter reporter, JobConf job, FileSystem fs)
    throws IOException
  {
    super(in, split, reporter, job, fs);
    gzipped_ = StreamInputFormat.isGzippedInput(job);
    if(gzipped_) {
      din_ = new DataInputStream(new GZIPInputStream(in_));
    } else {
      din_ = in_;
    }
  }

  public void seekNextRecordBoundary() throws IOException
  {
    if(gzipped_) {
      // no skipping: use din_ as-is 
      // assumes splitter created only one split per file
      return;
    } else {
      int bytesSkipped = 0;
      if (start_ != 0) {
        in_.seek(start_ - 1);
        // scan to the next newline in the file
        while (in_.getPos() < end_) {
          char c = (char)in_.read();
          bytesSkipped++;
          if (c == '\r' || c == '\n') {
            break;
          }
        }
      }

      //System.out.println("getRecordReader start="+start_ + " end=" + end_ + " bytesSkipped"+bytesSkipped);
    }
  }

  public synchronized boolean next(Writable key, Writable value)
    throws IOException {
    if(gzipped_) {
      // figure EOS from readLine
    } else {
      long pos = in_.getPos();
      if (pos >= end_)
        return false;
    }

    //((LongWritable)key).set(pos);      // key is position
    //((UTF8)value).set(readLine(in));   // value is line
    String line = readLine(din_);
    if(line == null) {
        return false; // for gzipped_
    }

    // key is line up to TAB, value is rest
    final boolean NOVAL = false;
    if(NOVAL) {
        ((UTF8)key).set(line);
        ((UTF8)value).set("");
    } else {
      int tab = line.indexOf('\t');
      if(tab == -1) {
        ((UTF8)key).set(line);
        ((UTF8)value).set("");
      } else {
        ((UTF8)key).set(line.substring(0, tab));
        ((UTF8)value).set(line.substring(tab+1));
      }
    }
    numRecStats(line);
    return true;
  }


  // from TextInputFormat
  private static String readLine(InputStream in) throws IOException {
    StringBuffer buffer = new StringBuffer();
    boolean over = true;
    while (true) {

      int b = in.read();
      if (b == -1)
        break;
      
      over = false;
      char c = (char)b;              // bug: this assumes eight-bit characters.
      if (c == '\r' || c == '\n')    // TODO || c == '\t' here
        break;

      buffer.append(c);
    }
    
    if(over) {
      return null;
    } else {
      return buffer.toString();
    }
    
  }

  boolean gzipped_;
  GZIPInputStream zin_;
  DataInputStream din_; // GZIP or plain
}
