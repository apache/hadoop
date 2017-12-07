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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation which selects a random file and truncates a random amount of bytes
 * (selected from the configuration for truncate size) from that file,
 * if it exists.
 * 
 * This operation will capture statistics on success for bytes written, time
 * taken (milliseconds), and success count and on failure it will capture the
 * number of failures and the time taken (milliseconds) to fail.
 */
class TruncateOp extends Operation {

  private static final Logger LOG = LoggerFactory.getLogger(TruncateOp.class);

  TruncateOp(ConfigExtractor cfg, Random rnd) {
    super(TruncateOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the file to truncate from
   * 
   * @return Path
   */
  protected Path getTruncateFile() {
    Path fn = getFinder().getFile();
    return fn;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    try {
      Path fn = getTruncateFile();
      boolean waitOnTruncate = getConfig().shouldWaitOnTruncate();
      long currentSize = fs.getFileStatus(fn).getLen();
      // determine file status for file length requirement
      // to know if should fill in partial bytes
      Range<Long> truncateSizeRange = getConfig().getTruncateSize();
      if (getConfig().shouldTruncateUseBlockSize()) {
        truncateSizeRange = getConfig().getBlockSize();
      }
      long truncateSize = Math.max(0L,
          currentSize - Range.betweenPositive(getRandom(), truncateSizeRange));
      long timeTaken = 0;
      LOG.info("Attempting to truncate file at " + fn + " to size "
          + Helper.toByteInfo(truncateSize));
      {
        // truncate
        long startTime = Timer.now();
        boolean completed = fs.truncate(fn, truncateSize);
        if(!completed && waitOnTruncate)
          waitForRecovery(fs, fn, truncateSize);
        timeTaken += Timer.elapsed(startTime);
      }
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.BYTES_WRITTEN, 0));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.OK_TIME_TAKEN, timeTaken));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.SUCCESSES, 1L));
      LOG.info("Truncate file " + fn + " to " + Helper.toByteInfo(truncateSize)
          + " in " + timeTaken + " milliseconds");
    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with truncating", e);
    } catch (IOException | UnsupportedOperationException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with truncating", e);
    }
    return out;
  }

  private void waitForRecovery(FileSystem fs, Path fn, long newLength)
      throws IOException {
    LOG.info("Waiting on truncate file recovery for " + fn);
    for(;;) {
      FileStatus stat = fs.getFileStatus(fn);
      if(stat.getLen() == newLength) break;
      try {Thread.sleep(1000);} catch(InterruptedException ignored) {}
    }
  }
}
