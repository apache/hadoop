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

package org.apache.hadoop.mapred.pipes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class is used to talk to a C++ reduce task.
 */
class PipesReducer implements Reducer {
  private static final Log LOG= LogFactory.getLog(PipesReducer.class.getName());
  private JobConf job;
  private Application application = null;
  private DownwardProtocol downlink = null;
  private boolean isOk = true;

  public void configure(JobConf job) {
    this.job = job;
  }

  /**
   * Process all of the keys and values. Start up the application if we haven't
   * started it yet.
   */
  public void reduce(WritableComparable key, Iterator values, 
                     OutputCollector output, Reporter reporter
                     ) throws IOException {
    isOk = false;
    startApplication(output, reporter);
    downlink.reduceKey(key);
    while (values.hasNext()) {
      downlink.reduceValue((Writable) values.next());
    }
    isOk = true;
  }

  private void startApplication(OutputCollector output, Reporter reporter) throws IOException {
    if (application == null) {
      try {
        LOG.info("starting application");
        application = new Application(job, output, reporter, 
                                      job.getOutputKeyClass(), 
                                      job.getOutputValueClass());
        downlink = application.getDownlink();
      } catch (InterruptedException ie) {
        throw new RuntimeException("interrupted", ie);
      }
      int reduce=0;
      downlink.runReduce(reduce, Submitter.getIsJavaRecordWriter(job));
    }
  }

  /**
   * Handle the end of the input by closing down the application.
   */
  public void close() throws IOException {
    // if we haven't started the application, we have nothing to do
    if (isOk) {
      OutputCollector nullCollector = new OutputCollector() {
        public void collect(WritableComparable key, 
                            Writable value) throws IOException {
          // NULL
        }
      };
      startApplication(nullCollector, Reporter.NULL);
    }
    try {
      if (isOk) {
        application.getDownlink().close();
      } else {
        application.getDownlink().abort();
      }
      LOG.info("waiting for finish");
      application.waitForFinish();
      LOG.info("got done");
    } catch (Throwable t) {
      application.abort(t);
    } finally {
      application.cleanup();
    }
  }
}
