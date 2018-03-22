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

package org.apache.hadoop.fs.slive;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The slive reducer which iterates over the given input values and merges them
 * together into a final output value.
 */
public class SliveReducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(SliveReducer.class);

  private ConfigExtractor config;

  /**
   * Logs to the given reporter and logs to the internal logger at info level
   * 
   * @param r
   *          the reporter to set status on
   * @param msg
   *          the message to log
   */
  private void logAndSetStatus(Reporter r, String msg) {
    r.setStatus(msg);
    LOG.info(msg);
  }

  /**
   * Fetches the config this object uses
   * 
   * @return ConfigExtractor
   */
  private ConfigExtractor getConfig() {
    return config;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
   * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
   * org.apache.hadoop.mapred.Reporter)
   */
  @Override // Reducer
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    OperationOutput collector = null;
    int reduceAm = 0;
    int errorAm = 0;
    logAndSetStatus(reporter, "Iterating over reduction values for key " + key);
    while (values.hasNext()) {
      Text value = values.next();
      try {
        OperationOutput val = new OperationOutput(key, value);
        if (collector == null) {
          collector = val;
        } else {
          collector = OperationOutput.merge(collector, val);
        }
        LOG.info("Combined " + val + " into/with " + collector);
        ++reduceAm;
      } catch (Exception e) {
        ++errorAm;
        logAndSetStatus(reporter, "Error iterating over reduction input "
            + value + " due to : " + StringUtils.stringifyException(e));
        if (getConfig().shouldExitOnFirstError()) {
          break;
        }
      }
    }
    logAndSetStatus(reporter, "Reduced " + reduceAm + " values with " + errorAm
        + " errors");
    if (collector != null) {
      logAndSetStatus(reporter, "Writing output " + collector.getKey() + " : "
          + collector.getOutputValue());
      output.collect(collector.getKey(), collector.getOutputValue());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
   * .JobConf)
   */
  @Override // MapReduceBase
  public void configure(JobConf conf) {
    config = new ConfigExtractor(conf);
    ConfigExtractor.dumpOptions(config);
  }

}
