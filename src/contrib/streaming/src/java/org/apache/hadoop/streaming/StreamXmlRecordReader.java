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

/** A way to interpret XML fragments as Mapper input records.
 *  Values are XML subtrees delimited by configurable tags.
 *  Keys could be the value of a certain attribute in the XML subtree, 
 *  but this is left to the stream processor application.
 *  @author Michel Tourn
 */
public class StreamXmlRecordReader extends StreamBaseRecordReader 
{
  public StreamXmlRecordReader(
    FSDataInputStream in, long start, long end, 
    String splitName, Reporter reporter, JobConf job)
    throws IOException
  {
    super(in, start, end, splitName, reporter, job);
    beginMark_ = checkJobGet("stream.recordreader.begin");
    endMark_   = checkJobGet("stream.recordreader.end");
  }

  String checkJobGet(String prop) throws IOException
  {
  	String val = job_.get(prop);
  	if(val == null) {
  		throw new IOException("JobConf: missing required property: " + prop);
  	}
  	return val;
  }
  
  public void seekNextRecordBoundary() throws IOException
  {
  System.out.println("@@@start seekNext " + in_.getPos());
    readUntilMatch(beginMark_, null);      
  System.out.println("@@@end   seekNext " + in_.getPos());
  }
    
  public synchronized boolean next(Writable key, Writable value)
   throws IOException
  {
    long pos = in_.getPos();
    if (pos >= end_)
      return false;
    
    StringBuffer buf = new StringBuffer();
    readUntilMatch(endMark_, buf);
    numRecStats(buf);
    return true;
  }

  void readUntilMatch(String pat, StringBuffer outBuf) throws IOException 
  {
    
    char[] cpat = pat.toCharArray();
    int m = 0;
    int msup = cpat.length;
    while (true) {
      int b = in_.read();
      if (b == -1)
        break;

      char c = (char)b; // this assumes eight-bit matching. OK with UTF-8
      if (c == cpat[m]) {
        m++;
        if(m==msup-1) {
          break;
        }
      } else {
        m = 0;
      }
      if(outBuf != null) {
        outBuf.append(c);
      }
    }
System.out.println("@@@START readUntilMatch(" + pat + ", " + outBuf + "\n@@@END readUntilMatch");
  }
  
  
  String beginMark_;
  String endMark_;
}
