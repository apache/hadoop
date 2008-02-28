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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.net.URLDecoder;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.io.Writable;

/** A generic Reducer bridge.
 *  It delegates operations to an external program via stdin and stdout.
 */
public class PipeReducer extends PipeMapRed implements Reducer {

  String getPipeCommand(JobConf job) {
    String str = job.get("stream.reduce.streamprocessor");
    if (str == null) {
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.err.println("stream.reduce.streamprocessor in jobconf not found");
      return null;
    }
  }

  boolean getDoPipe() {
    String argv = getPipeCommand(job_);
    // Currently: null is identity reduce. REDUCE_NONE is no-map-outputs.
    return (argv != null) && !StreamJob.REDUCE_NONE.equals(argv);
  }

  public void reduce(Object key, Iterator values, OutputCollector output,
                     Reporter reporter) throws IOException {

    // init
    if (doPipe_ && outThread_ == null) {
      startOutputThreads(output, reporter);
    }
    try {
      while (values.hasNext()) {
        Writable val = (Writable) values.next();
        numRecRead_++;
        maybeLogRecord();
        if (doPipe_) {
          if (outerrThreadsThrowable != null) {
            mapRedFinished();
            throw new IOException ("MROutput/MRErrThread failed:"
                                   + StringUtils.stringifyException(
                                                                    outerrThreadsThrowable));
          }
          write(key);
          clientOut_.write('\t');
          write(val);
          clientOut_.write('\n');
          clientOut_.flush();
        } else {
          // "identity reduce"
          output.collect(key, val);
        }
      }
    } catch (IOException io) {
      // a common reason to get here is failure of the subprocess.
      // Document that fact, if possible.
      String extraInfo = "";
      try {
        int exitVal = sim.exitValue();
	if (exitVal == 0) {
	  extraInfo = "subprocess exited successfully\n";
	} else {
	  extraInfo = "subprocess exited with error code " + exitVal + "\n";
	};
      } catch (IllegalThreadStateException e) {
        // hmm, but child is still running.  go figure.
	extraInfo = "subprocess still running\n";
      };
      appendLogToJobLog("failure");
      mapRedFinished();
      throw new IOException(extraInfo + getContext() + io.getMessage());
    }
  }

  public void close() {
    appendLogToJobLog("success");
    mapRedFinished();
  }

  @Override
  char getFieldSeparator() {
    return super.reduceOutFieldSeparator;
  }

  @Override
  int getNumOfKeyFields() {
    return super.numOfReduceOutputKeyFields;
  }

}
