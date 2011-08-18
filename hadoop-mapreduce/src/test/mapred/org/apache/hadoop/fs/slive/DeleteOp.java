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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * Operation which selects a random file and attempts to delete that file (if it
 * exists)
 * 
 * This operation will capture statistics on success the time taken to delete
 * and the number of successful deletions that occurred and on failure or error
 * it will capture the number of failures and the amount of time taken to fail
 */
class DeleteOp extends Operation {

  private static final Log LOG = LogFactory.getLog(DeleteOp.class);

  DeleteOp(ConfigExtractor cfg, Random rnd) {
    super(DeleteOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the file to delete
   */
  protected Path getDeleteFile() {
    Path fn = getFinder().getFile();
    return fn;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    try {
      Path fn = getDeleteFile();
      long timeTaken = 0;
      boolean deleteStatus = false;
      {
        long startTime = Timer.now();
        deleteStatus = fs.delete(fn, false);
        timeTaken = Timer.elapsed(startTime);
      }
      // collect the stats
      if (!deleteStatus) {
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.FAILURES, 1L));
        LOG.info("Could not delete " + fn);
      } else {
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.OK_TIME_TAKEN, timeTaken));
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.SUCCESSES, 1L));
        LOG.info("Could delete " + fn);
      }
    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with deleting", e);
    } catch (IOException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with deleting", e);
    }
    return out;
  }

}
