/*
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

package org.apache.hadoop.fs.s3a.commit;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

/**
 * Implementation of the fault injection lifecycle.
 * Can reset a fault on failure or always raise it.
 */
public final class CommitterFaultInjectionImpl
    extends PathOutputCommitter implements CommitterFaultInjection {

  private Set<Faults> faults;
  private boolean resetOnFailure;

  public CommitterFaultInjectionImpl(Path outputPath,
      JobContext context,
      boolean resetOnFailure,
      Faults... faults) throws IOException {
    super(outputPath, context);
    setFaults(faults);
    this.resetOnFailure = resetOnFailure;
  }

  @Override
  public void setFaults(Faults... faults) {
    this.faults = new HashSet<>(faults.length);
    Collections.addAll(this.faults, faults);
  }

  /**
   * Fail if the condition is in the set of faults, may optionally reset
   * it before failing.
   * @param condition condition to check for
   * @throws Failure if the condition is faulting
   */
  private void maybeFail(Faults condition) throws Failure {
    if (faults.contains(condition)) {
      if (resetOnFailure) {
        faults.remove(condition);
      }
      throw new Failure();
    }
  }

  @Override
  public Path getWorkPath() throws IOException {
    maybeFail(Faults.getWorkPath);
    return null;
  }

  @Override
  public Path getOutputPath() {
    return null;
  }


  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    maybeFail(Faults.setupJob);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    maybeFail(Faults.setupTask);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    maybeFail(Faults.needsTaskCommit);
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    maybeFail(Faults.commitTask);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    maybeFail(Faults.abortTask);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    maybeFail(Faults.commitJob);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state)
      throws IOException {
    maybeFail(Faults.abortJob);
  }

  /**
   * The exception raised on failure.
   */
  public static class Failure extends IOException {
    public Failure() {
      super(COMMIT_FAILURE_MESSAGE);
    }
  }

}
