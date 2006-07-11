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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


public class StreamSequenceRecordReader extends StreamBaseRecordReader
{

  public StreamSequenceRecordReader (
    FSDataInputStream in, FileSplit split, Reporter reporter, JobConf job, FileSystem fs)
    throws IOException
  {
    super(in, split, reporter, job, fs);
    numFailed_ = 0;
    // super.in_ ignored, using rin_ instead
  }
  
    
  public synchronized boolean next(Writable key, Writable value)
   throws IOException
  {         
    boolean success;
    do {    
      if (!more_) return false;
      success = false;
      try {
        long pos = rin_.getPosition();
        boolean eof = rin_.next(key, value);
        if (pos >= end_ && rin_.syncSeen()) {
          more_ = false;
        } else {
          more_ = eof;
        }
        success = true;
      } catch(IOException io) {
        numFailed_++;
        if(numFailed_ < 100 || numFailed_ % 100 == 0) {
          err_.println("StreamSequenceRecordReader: numFailed_/numRec_=" 
            + numFailed_+ "/" + numRec_);
        }
        io.printStackTrace(err_);
        success = false;
      }
    } while(!success);
    numRecStats("");
    return more_;    
  }
  

  public void seekNextRecordBoundary() throws IOException
  {
    rin_ = new SequenceFile.Reader(fs_, split_.getPath(), job_);
    end_ = split_.getStart() + split_.getLength();

    if (split_.getStart() > rin_.getPosition())
      rin_.sync(split_.getStart());                  // sync to start

    more_ = rin_.getPosition() < end_;

    reporter_.setStatus(split_.toString());

    //return new SequenceFileRecordReader(job_, split_);
  }

  boolean more_;
  SequenceFile.Reader rin_;
  int numFailed_;
  PrintStream err_ = System.err;

}
