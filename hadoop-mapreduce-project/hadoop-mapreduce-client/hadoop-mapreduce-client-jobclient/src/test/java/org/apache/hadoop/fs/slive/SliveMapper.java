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
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The slive class which sets up the mapper to be used which itself will receive
 * a single dummy key and value and then in a loop run the various operations
 * that have been selected and upon operation completion output the collected
 * output from that operation (and repeat until finished).
 */
public class SliveMapper extends MapReduceBase implements
    Mapper<Object, Object, Text, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(SliveMapper.class);

  private static final String OP_TYPE = SliveMapper.class.getSimpleName();

  private FileSystem filesystem;
  private ConfigExtractor config;
  private int taskId;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
   * .JobConf)
   */
  @Override // MapReduceBase
  public void configure(JobConf conf) {
    try {
      config = new ConfigExtractor(conf);
      ConfigExtractor.dumpOptions(config);
      filesystem = config.getBaseDirectory().getFileSystem(conf);
    } catch (Exception e) {
      LOG.error("Unable to setup slive " + StringUtils.stringifyException(e));
      throw new RuntimeException("Unable to setup slive configuration", e);
    }
    if(conf.get(MRJobConfig.TASK_ATTEMPT_ID) != null ) {
      this.taskId = TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID))
        .getTaskID().getId();
    } else {
      // So that branch-1/0.20 can run this same code as well
      this.taskId = TaskAttemptID.forName(conf.get("mapred.task.id"))
          .getTaskID().getId();
    }
  }

  /**
   * Fetches the config this object uses
   * 
   * @return ConfigExtractor
   */
  private ConfigExtractor getConfig() {
    return config;
  }

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
   * Runs the given operation and reports on its results
   * 
   * @param op
   *          the operation to run
   * @param reporter
   *          the status reporter to notify
   * @param output
   *          the output to write to
   * @throws IOException
   */
  private void runOperation(Operation op, Reporter reporter,
      OutputCollector<Text, Text> output, long opNum) throws IOException {
    if (op == null) {
      return;
    }
    logAndSetStatus(reporter, "Running operation #" + opNum + " (" + op + ")");
    List<OperationOutput> opOut = op.run(filesystem);
    logAndSetStatus(reporter, "Finished operation #" + opNum + " (" + op + ")");
    if (opOut != null && !opOut.isEmpty()) {
      for (OperationOutput outData : opOut) {
        output.collect(outData.getKey(), outData.getOutputValue());
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
   * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
   * org.apache.hadoop.mapred.Reporter)
   */
  @Override // Mapper
  public void map(Object key, Object value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    logAndSetStatus(reporter, "Running slive mapper for dummy key " + key
        + " and dummy value " + value);
    //Add taskID to randomSeed to deterministically seed rnd.
    Random rnd = config.getRandomSeed() != null ?
      new Random(this.taskId + config.getRandomSeed()) : new Random();
    WeightSelector selector = new WeightSelector(config, rnd);
    long startTime = Timer.now();
    long opAm = 0;
    long sleepOps = 0;
    int duration = getConfig().getDurationMilliseconds();
    Range<Long> sleepRange = getConfig().getSleepRange();
    Operation sleeper = null;
    if (sleepRange != null) {
      sleeper = new SleepOp(getConfig(), rnd);
    }
    while (Timer.elapsed(startTime) < duration) {
      try {
        logAndSetStatus(reporter, "Attempting to select operation #"
            + (opAm + 1));
        int currElapsed = (int) (Timer.elapsed(startTime));
        Operation op = selector.select(currElapsed, duration);
        if (op == null) {
          // no ops left
          break;
        } else {
          // got a good op
          ++opAm;
          runOperation(op, reporter, output, opAm);
        }
        // do a sleep??
        if (sleeper != null) {
          // these don't count against the number of operations
          ++sleepOps;
          runOperation(sleeper, reporter, output, sleepOps);
        }
      } catch (Exception e) {
        logAndSetStatus(reporter, "Failed at running due to "
            + StringUtils.stringifyException(e));
        if (getConfig().shouldExitOnFirstError()) {
          break;
        }
      }
    }
    // write out any accumulated mapper stats
    {
      long timeTaken = Timer.elapsed(startTime);
      OperationOutput opCount = new OperationOutput(OutputType.LONG, OP_TYPE,
          ReportWriter.OP_COUNT, opAm);
      output.collect(opCount.getKey(), opCount.getOutputValue());
      OperationOutput overallTime = new OperationOutput(OutputType.LONG,
          OP_TYPE, ReportWriter.OK_TIME_TAKEN, timeTaken);
      output.collect(overallTime.getKey(), overallTime.getOutputValue());
      logAndSetStatus(reporter, "Finished " + opAm + " operations in "
          + timeTaken + " milliseconds");
    }
  }
}
