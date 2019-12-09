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
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.DataWriter.GenerateOutput;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation which selects a random file and appends a random amount of bytes
 * (selected from the configuration for append size) to that file if it exists.
 * 
 * This operation will capture statistics on success for bytes written, time
 * taken (milliseconds), and success count and on failure it will capture the
 * number of failures and the time taken (milliseconds) to fail.
 */
class AppendOp extends Operation {

  private static final Logger LOG = LoggerFactory.getLogger(AppendOp.class);

  AppendOp(ConfigExtractor cfg, Random rnd) {
    super(AppendOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the file to append to
   * 
   * @return Path
   */
  protected Path getAppendFile() {
    Path fn = getFinder().getFile();
    return fn;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    OutputStream os = null;
    try {
      Path fn = getAppendFile();
      // determine file status for file length requirement
      // to know if should fill in partial bytes
      Range<Long> appendSizeRange = getConfig().getAppendSize();
      if (getConfig().shouldAppendUseBlockSize()) {
        appendSizeRange = getConfig().getBlockSize();
      }
      long appendSize = Range.betweenPositive(getRandom(), appendSizeRange);
      long timeTaken = 0, bytesAppended = 0;
      DataWriter writer = new DataWriter(getRandom());
      LOG.info("Attempting to append to file at " + fn + " of size "
          + Helper.toByteInfo(appendSize));
      {
        // open
        long startTime = Timer.now();
        os = fs.append(fn);
        timeTaken += Timer.elapsed(startTime);
        // append given length
        GenerateOutput stats = writer.writeSegment(appendSize, os);
        timeTaken += stats.getTimeTaken();
        bytesAppended += stats.getBytesWritten();
        // capture close time
        startTime = Timer.now();
        os.close();
        os = null;
        timeTaken += Timer.elapsed(startTime);
      }
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.BYTES_WRITTEN, bytesAppended));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.OK_TIME_TAKEN, timeTaken));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.SUCCESSES, 1L));
      LOG.info("Appended " + Helper.toByteInfo(bytesAppended) + " to file "
          + fn + " in " + timeTaken + " milliseconds");
    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with appending", e);
    } catch (IOException | UnsupportedOperationException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with appending", e);
    } finally {
      if (os != null) {
        try {
          os.close();
        } catch (IOException e) {
          LOG.warn("Error with closing append stream", e);
        }
      }
    }
    return out;
  }
}
