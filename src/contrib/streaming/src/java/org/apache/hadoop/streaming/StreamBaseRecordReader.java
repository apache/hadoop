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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LogFormatter;

/** 
 * Shared functionality for hadoopStreaming formats.
 * A custom reader can be defined to be a RecordReader with the constructor below
 * and is selected with the option bin/hadoopStreaming -inputreader ...
 * @see StreamLineRecordReader
 * @see StreamXmlRecordReader 
 * @author Michel Tourn
 */
public abstract class StreamBaseRecordReader implements RecordReader
{
    
  protected static final Logger LOG = LogFormatter.getLogger(StreamBaseRecordReader.class.getName());

  public StreamBaseRecordReader(
    FSDataInputStream in, long start, long end, 
    String splitName, Reporter reporter, JobConf job)
    throws IOException
  {
    in_ = in;
    start_ = start;
    splitName_ = splitName;
    end_ = end;
    reporter_ = reporter;
    job_ = job;
  }

  /** Called once before the first call to next */
  public void init() throws IOException
  {
    seekNextRecordBoundary();
  }
  
  /** Implementation should seek forward in_ to the first byte of the next record.
   *  The initial byte offset in the stream is arbitrary.
   */
  public abstract void seekNextRecordBoundary() throws IOException;
  
  
  /** Read a record. Implementation should call numRecStats at the end
   */  
  public abstract boolean next(Writable key, Writable value) throws IOException;

  
  void numRecStats(CharSequence record) throws IOException
  {
    numRec_++;          
    if(numRec_ == nextStatusRec_) {
      nextStatusRec_ +=100000;//*= 10;
      String status = getStatus(record);
      LOG.info(status);
      reporter_.setStatus(status);
    }
  }

 long lastMem =0;
 String getStatus(CharSequence record)
 {
    long pos = -1;
    try { 
      pos = getPos();
    } catch(IOException io) {
    }
    final int M = 2000;
    String recStr;
    if(record.length() > M) {
    	recStr = record.subSequence(0, M) + "...";
    } else {
    	recStr = record.toString();
    }
    String status = "HSTR " + StreamUtil.HOST + " " + numRec_ + ". pos=" + pos + " Processing record=" + recStr;
    status += " " + splitName_;
    return status;
  }

  /** Returns the current position in the input. */
  public synchronized long getPos() throws IOException 
  { 
    return in_.getPos(); 
  }

  /** Close this to future operations.*/
  public synchronized void close() throws IOException 
  { 
    in_.close(); 
  }

  FSDataInputStream in_;
  long start_;
  long end_;
  String splitName_;
  Reporter reporter_;
  JobConf job_;
  int numRec_ = 0;
  int nextStatusRec_ = 1;
  
}
