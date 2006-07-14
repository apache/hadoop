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

import java.io.IOException;
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

/** A generic Reducer bridge.
 *  It delegates operations to an external program via stdin and stdout.
 *  @author Michel Tourn
 */
public class PipeReducer extends PipeMapRed implements Reducer
{

  String getPipeCommand(JobConf job)
  {
    return job.get("stream.reduce.streamprocessor");
  }

  String getKeyColPropName()
  {
    return "reduceKeyCols";
  }  
  
  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    // init
    if(doPipe_ && outThread_ == null) {
      startOutputThreads(output, reporter);
    }
    try {
      while (values.hasNext()) {
        Writable val = (Writable)values.next();
        numRecRead_++;
        maybeLogRecord();
        if(doPipe_) {
          clientOut_.writeBytes(key.toString());
          clientOut_.writeBytes("\t");
          clientOut_.writeBytes(val.toString());
          clientOut_.writeBytes("\n");
          clientOut_.flush();
        } else {
          // "identity reduce"
          output.collect(key, val);
        }
      }
    } catch(IOException io) {
      appendLogToJobLog("failure");
      throw new IOException(getContext() + io.getMessage());    
    }
  }

  public void close()
  {
    appendLogToJobLog("success");
    mapRedFinished();
  }

}
