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
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/** A generic Mapper bridge.
 *  It delegates operations to an external program via stdin and stdout.
 *  @author Michel Tourn
 */
public class PipeMapper extends PipeMapRed implements Mapper
{

  String getPipeCommand(JobConf job)
  {
    return job.get("stream.map.streamprocessor");
  }

  String getKeyColPropName()
  {
    return "mapKeyCols";
  }  


  // Do NOT declare default constructor
  // (MapRed creates it reflectively)

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException
  {
    // init
    if(outThread_ == null) {
      startOutputThreads(output, reporter);
    }
    try {
      // 1/4 Hadoop in
      mapredKey_ = key.toString();
      numRecRead_++;

      maybeLogRecord();

      // 2/4 Hadoop to Tool
      if(numExceptions_==0) {
        clientOut_.writeBytes(mapredKey_);
        clientOut_.writeBytes("\t");
        clientOut_.writeBytes(value.toString());
        clientOut_.writeBytes("\n");
        clientOut_.flush();
      } else {
        numRecSkipped_++;
      }
    } catch(IOException io) {
      numExceptions_++;
      if(numExceptions_ > 1 || numRecWritten_ < minRecWrittenToEnableSkip_) {
        // terminate with failure
        String msg = logFailure(io);
        appendLogToJobLog("failure");
        throw new IOException(msg);
      } else {
        // terminate with success:
        // swallow input records although the stream processor failed/closed
      }
    }
  }
  
  
  public void close()
  {
    appendLogToJobLog("success");
    mapRedFinished();
  }

}
