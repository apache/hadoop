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

import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * Operation which sleeps for a given number of milliseconds according to the
 * config given, and reports on the sleep time overall
 */
class SleepOp extends Operation {

  private static final Log LOG = LogFactory.getLog(SleepOp.class);

  SleepOp(ConfigExtractor cfg, Random rnd) {
    super(SleepOp.class.getSimpleName(), cfg, rnd);
  }

  protected long getSleepTime(Range<Long> sleepTime) {
    long sleepMs = Range.betweenPositive(getRandom(), sleepTime);
    return sleepMs;
  }

  /**
   * Sleep for a random amount of time between a given positive range
   * 
   * @param sleepTime
   *          positive long range for times to choose
   * 
   * @return output data on operation
   */
  List<OperationOutput> run(Range<Long> sleepTime) {
    List<OperationOutput> out = super.run(null);
    try {
      if (sleepTime != null) {
        long sleepMs = getSleepTime(sleepTime);
        long startTime = Timer.now();
        sleep(sleepMs);
        long elapsedTime = Timer.elapsed(startTime);
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.OK_TIME_TAKEN, elapsedTime));
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.SUCCESSES, 1L));
      }
    } catch (InterruptedException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with sleeping", e);
    }
    return out;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    Range<Long> sleepTime = getConfig().getSleepRange();
    return run(sleepTime);
  }

  /**
   * Sleeps the current thread for X milliseconds
   * 
   * @param ms
   *          milliseconds to sleep for
   * 
   * @throws InterruptedException
   */
  private void sleep(long ms) throws InterruptedException {
    if (ms <= 0) {
      return;
    }
    Thread.sleep(ms);
  }
}
