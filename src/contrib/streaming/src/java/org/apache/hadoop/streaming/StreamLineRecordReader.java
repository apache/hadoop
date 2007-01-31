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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Similar to org.apache.hadoop.mapred.TextRecordReader, 
 * but delimits key and value with a TAB.
 * @author Michel Tourn
 */
public class StreamLineRecordReader extends LineRecordReader {
  
  private String splitName;
  private Reporter reporter;
  private FileSplit split;  
  private int numRec = 0;
  private int nextStatusRec = 1;
  private int statusMaxRecordChars;
  protected static final Log LOG = LogFactory.getLog(StreamLineRecordReader.class);
  // base class uses LongWritable as key, use this. 
  private WritableComparable dummyKey = super.createKey(); 
  private Text innerValue = (Text)super.createValue(); 

  public StreamLineRecordReader(FSDataInputStream in, FileSplit split, 
      Reporter reporter,
      JobConf job, FileSystem fs) throws IOException {
    super(createStream(in, job), split.getStart(), 
        split.getStart() + split.getLength());
    this.split = split ; 
    this.reporter = reporter ; 
  }
  
  private static InputStream createStream(FSDataInputStream in, JobConf job) 
    throws IOException{
    InputStream finalStream = in ;
    boolean gzipped = StreamInputFormat.isGzippedInput(job);
    if ( gzipped ) {
      GzipCodec codec = new GzipCodec();
      codec.setConf(job);
      finalStream = codec.createInputStream(in);
    } 
    return finalStream; 
  }
  
  public WritableComparable createKey() {
    return new Text();
  }  
  
  public Writable createValue() {
    return new Text();
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
    byte[] line = null ; 
    if( super.next(dummyKey, innerValue) ){
      line = innerValue.getBytes(); 
    }else{
      return false;
    }
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
  
  private void numRecStats(byte[] record, int start, int len) throws IOException {
    numRec++;
    if (numRec == nextStatusRec) {
      String recordStr = new String(record, start, Math.min(len, statusMaxRecordChars), "UTF-8");
      nextStatusRec += 100;//*= 10;
      String status = getStatus(recordStr);
      LOG.info(status);
      reporter.setStatus(status);
    }
  }

  private String getStatus(CharSequence record) {
    long pos = -1;
    try {
      pos = getPos();
    } catch (IOException io) {
    }
    String recStr;
    if (record.length() > statusMaxRecordChars) {
      recStr = record.subSequence(0, statusMaxRecordChars) + "...";
    } else {
      recStr = record.toString();
    }
    String unqualSplit = split.getFile().getName() + ":" + split.getStart() + "+"
        + split.getLength();
    String status = "HSTR " + StreamUtil.HOST + " " + numRec + ". pos=" + pos + " " + unqualSplit
        + " Processing record=" + recStr;
    status += " " + splitName;
    return status;
  }
}
