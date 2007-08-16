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

package org.apache.hadoop.examples;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.aggregate.*;

/**
 * This is an example Aggregated Hadoop Map/Reduce application. It reads the
 * text input files, breaks each line into words and counts them. The output is
 * a locally sorted list of words and the count of how often they occurred.
 * 
 * To run: bin/hadoop jar hadoop-*-examples.jar aggregatewordcount <i>in-dir</i>
 * <i>out-dir</i> <i>numOfReducers</i> textinputformat
 * 
 */
public class AggregateWordCount {

  public static class WordCountPlugInClass extends
      ValueAggregatorBaseDescriptor {
    public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                            Object val) {
      String countType = LONG_VALUE_SUM;
      ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
      String line = val.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        Entry<Text, Text> e = generateEntry(countType, itr.nextToken(), ONE);
        if (e != null) {
          retv.add(e);
        }
      }
      return retv;
    }
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   * 
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = ValueAggregatorJob.createValueAggregatorJob(args);
    //specify the number of aggregators to be used
    conf.setInt("aggregator.descriptor.num", 1);
    //specify the aggregator descriptor
    conf
        .set(
            "aggregator.descriptor.num.0",
            "UserDefined,org.apache.hadoop.examples.AggregateWordCount.WordCountPlugInClass");
    JobClient.runJob(conf);
  }

}
