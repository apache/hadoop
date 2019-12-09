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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation which selects a random file and a second random file and attempts
 * to rename that first file into the second file.
 * 
 * This operation will capture statistics on success the time taken to rename
 * those files and the number of successful renames that occurred and on failure
 * or error it will capture the number of failures and the amount of time taken
 * to fail
 */
class RenameOp extends Operation {

  /**
   * Class that holds the src and target for renames
   */
  protected static class SrcTarget {
    private Path src, target;

    SrcTarget(Path src, Path target) {
      this.src = src;
      this.target = target;
    }

    Path getSrc() {
      return src;
    }

    Path getTarget() {
      return target;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RenameOp.class);

  RenameOp(ConfigExtractor cfg, Random rnd) {
    super(RenameOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the file names to rename
   * 
   * @return SrcTarget
   */
  protected SrcTarget getRenames() {
    Path src = getFinder().getFile();
    Path target = getFinder().getFile();
    return new SrcTarget(src, target);
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    try {
      // find the files to modify
      SrcTarget targets = getRenames();
      Path src = targets.getSrc();
      Path target = targets.getTarget();
      // capture results
      boolean renamedOk = false;
      long timeTaken = 0;
      {
        // rename it
        long startTime = Timer.now();
        renamedOk = fs.rename(src, target);
        timeTaken = Timer.elapsed(startTime);
      }
      if (renamedOk) {
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.OK_TIME_TAKEN, timeTaken));
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.SUCCESSES, 1L));
        LOG.info("Renamed " + src + " to " + target);
      } else {
        out.add(new OperationOutput(OutputType.LONG, getType(),
            ReportWriter.FAILURES, 1L));
        LOG.warn("Could not rename " + src + " to " + target);
      }
    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with renaming", e);
    } catch (IOException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with renaming", e);
    }
    return out;
  }
}
