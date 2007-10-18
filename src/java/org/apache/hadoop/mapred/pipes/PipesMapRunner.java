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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An adaptor to run a C++ mapper.
 */
class PipesMapRunner<K1 extends WritableComparable, V1 extends Writable,
    K2 extends WritableComparable, V2 extends Writable>
    extends MapRunner<K1, V1, K2, V2> {
  private JobConf job;

  /**
   * Get the new configuration.
   * @param job the job's configuration
   */
  public void configure(JobConf job) {
    this.job = job;
  }

  /**
   * Run the map task.
   * @param input the set of inputs
   * @param output the object to collect the outputs of the map
   * @param reporter the object to update with status
   */
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter) throws IOException {
    Application<K1, V1, K2, V2> application = null;
    try {
      application = new Application<K1, V1, K2, V2>(job, output, reporter,
                                    job.getMapOutputKeyClass(),
                                    job.getMapOutputValueClass());
    } catch (InterruptedException ie) {
      throw new RuntimeException("interrupted", ie);
    }
    DownwardProtocol<K1, V1> downlink = application.getDownlink();
    boolean isJavaInput = Submitter.getIsJavaRecordReader(job);
    downlink.runMap(reporter.getInputSplit(), 
                    job.getNumReduceTasks(), isJavaInput);
    try {
      if (isJavaInput) {
        // allocate key & value instances that are re-used for all entries
        K1 key = input.createKey();
        V1 value = input.createValue();
        downlink.setInputTypes(key.getClass().getName(),
                               value.getClass().getName());
        
        while (input.next(key, value)) {
          // map pair to output
          downlink.mapItem(key, value);
        }
        downlink.endOfInput();
      }
      application.waitForFinish();
    } catch (Throwable t) {
      application.abort(t);
    } finally {
      application.cleanup();
    }
  }
  
}
