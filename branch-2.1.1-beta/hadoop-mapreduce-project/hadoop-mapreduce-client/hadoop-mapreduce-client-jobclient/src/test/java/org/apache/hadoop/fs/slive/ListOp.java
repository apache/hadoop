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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * Operation which selects a random directory and attempts to list that
 * directory (if it exists)
 * 
 * This operation will capture statistics on success the time taken to list that
 * directory and the number of successful listings that occurred as well as the
 * number of entries in the selected directory and on failure or error it will
 * capture the number of failures and the amount of time taken to fail
 */
class ListOp extends Operation {

  private static final Log LOG = LogFactory.getLog(ListOp.class);

  ListOp(ConfigExtractor cfg, Random rnd) {
    super(ListOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the directory to list
   * 
   * @return Path
   */
  protected Path getDirectory() {
    Path dir = getFinder().getDirectory();
    return dir;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    try {
      Path dir = getDirectory();
      long dirEntries = 0;
      long timeTaken = 0;
      {
        long startTime = Timer.now();
        FileStatus[] files = fs.listStatus(dir);
        timeTaken = Timer.elapsed(startTime);
        dirEntries = files.length;
      }
      // log stats
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.OK_TIME_TAKEN, timeTaken));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.SUCCESSES, 1L));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.DIR_ENTRIES, dirEntries));
      LOG.info("Directory " + dir + " has " + dirEntries + " entries");
    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with listing", e);
    } catch (IOException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with listing", e);
    }
    return out;
  }

}
