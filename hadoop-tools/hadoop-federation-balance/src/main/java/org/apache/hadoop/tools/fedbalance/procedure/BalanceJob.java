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
package org.apache.hadoop.tools.fedbalance.procedure;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

/**
 * A Job is a state machine consists of many procedures. The procedures are
 * executed as a chain. Each procedure needs to specify the next procedure. If
 * there is no next procedure then the job is finished.
 */
public final class BalanceJob<T extends BalanceProcedure> implements Writable {
  private String id;
  private BalanceProcedureScheduler scheduler;
  private volatile boolean jobDone = false;
  private Exception error;
  public static final Logger LOG = LoggerFactory.getLogger(BalanceJob.class);
  private Map<String, T> procedureTable = new HashMap<>();
  private T firstProcedure;
  private T curProcedure;
  private T lastProcedure;
  private boolean removeAfterDone;

  static final String NEXT_PROCEDURE_NONE = "NONE";
  private static Set<String> reservedNames = new HashSet<>();

  static {
    reservedNames.add(NEXT_PROCEDURE_NONE);
  }

  public static class Builder<T extends BalanceProcedure> {

    private List<T> procedures = new ArrayList<>();
    private boolean removeAfterDone = false;

    /**
     * Append a procedure to the tail.
     */
    public Builder nextProcedure(T procedure) {
      int size = procedures.size();
      if (size > 0) {
        procedures.get(size - 1).setNextProcedure(procedure.name());
      }
      procedure.setNextProcedure(NEXT_PROCEDURE_NONE);
      procedures.add(procedure);
      return this;
    }

    /**
     * Automatically remove this job from the scheduler cache when the job is
     * done.
     */
    public Builder removeAfterDone(boolean remove) {
      removeAfterDone = remove;
      return this;
    }

    public BalanceJob build() throws IOException {
      BalanceJob job = new BalanceJob(procedures, removeAfterDone);
      for (BalanceProcedure<T> p : procedures) {
        p.setJob(job);
      }
      return job;
    }
  }

  private BalanceJob(Iterable<T> procedures, boolean remove)
      throws IOException {
    for (T p : procedures) {
      String taskName = p.name();
      if (reservedNames.contains(taskName)) {
        throw new IOException(taskName + " is reserved.");
      }
      procedureTable.put(p.name(), p);
      if (firstProcedure == null) {
        firstProcedure = p;
      }
    }
    removeAfterDone = remove;
    lastProcedure = null;
    curProcedure = firstProcedure;
  }

  /**
   * Run the state machine.
   */
  public void execute() {
    boolean quit = false;
    try {
      while (!jobDone && !quit && scheduler.isRunning()) {
        if (curProcedure == null) { // Job done.
          finish(null);
          quit = true;
        } else {
          if (curProcedure == firstProcedure || lastProcedure != curProcedure) {
            LOG.info("Start procedure {}, last procedure is {}",
                curProcedure.name(),
                lastProcedure == null ? null : lastProcedure.name());
          }
          if (curProcedure.execute()) {
            lastProcedure = curProcedure;
            curProcedure = next();
          }
          if (!scheduler.writeJournal(this)) {
            quit = true; // Write journal failed. Simply quit because this job
                         // has already been added to the recoverQueue.
            LOG.debug("Write journal failed. Quit and wait for recovery.");
          }
        }
      }
    } catch (BalanceProcedure.RetryException tre) {
      scheduler.delay(this, curProcedure.delayMillisBeforeRetry());
    } catch (Exception e) {
      finish(e);
    } catch (Throwable t) {
      IOException err = new IOException("Got throwable error.", t);
      finish(err);
    }
  }

  private T next() {
    if (curProcedure == null) {
      return firstProcedure;
    } else {
      return procedureTable.get(curProcedure.nextProcedure());
    }
  }

  /**
   * Job finishes. It could be either success or failure.
   * @param exception the exception that causes the job to fail. null indicates
   *                 the job is successful.
   */
  private synchronized void finish(Exception exception) {
    assert !jobDone;
    if (scheduler.jobDone(this)) {
      jobDone = true;
      error = exception;
      notifyAll();
    }
  }

  void setScheduler(BalanceProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }

  void setId(String id) {
    this.id = id;
  }

  /**
   * Get the uid of the job.
   */
  public String getId() {
    return this.id;
  }

  /**
   * Whether this job should be removed after it's done.
   */
  @VisibleForTesting
  public boolean shouldRemoveAfterDone() {
    return removeAfterDone;
  }

  @VisibleForTesting
  void setLastProcedure(T lastProcedure) {
    this.lastProcedure = lastProcedure;
  }

  @VisibleForTesting
  void setCurrentProcedure(T currentProcedure) {
    this.curProcedure = currentProcedure;
  }

  /**
   * Return true if the job has finished.
   */
  public boolean isJobDone() {
    return jobDone;
  }

  /**
   * Wait until the job is done.
   */
  public synchronized void waitJobDone() throws InterruptedException {
    while (!jobDone) {
      wait();
    }
  }

  /**
   * Return the error exception during the job execution. This should be called
   * after the job finishes.
   */
  public Exception getError() {
    return error;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (id == null) {
      throw new IOException("BalanceJob with id=null can not be serialized.");
    }
    Text.writeString(out, id);
    int taskTableSize = procedureTable.size();
    out.writeInt(taskTableSize);
    for (T p : procedureTable.values()) {
      Text.writeString(out, p.getClass().getName());
      p.write(out);
    }
    if (firstProcedure != null) {
      Text.writeString(out, firstProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
    if (curProcedure != null) {
      Text.writeString(out, curProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
    if (lastProcedure != null) {
      Text.writeString(out, lastProcedure.name());
    } else {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = Text.readString(in);
    procedureTable = new HashMap<>();
    int taskTableSize = in.readInt();
    for (int i = 0; i < taskTableSize; i++) {
      String className = Text.readString(in);
      try {
        T p = (T) ReflectionUtils.newInstance(Class.forName(className), null);
        p.readFields(in);
        procedureTable.put(p.name(), p);
      } catch (Exception e) {
        LOG.error("Failed reading Procedure.", e);
        throw new IOException(e);
      }
    }
    String firstProcedureName = Text.readString(in);
    if (firstProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      firstProcedure = null;
    } else {
      firstProcedure = procedureTable.get(firstProcedureName);
    }
    String currentProcedureName = Text.readString(in);
    if (currentProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      curProcedure = null;
    } else {
      curProcedure = procedureTable.get(currentProcedureName);
    }
    String lastProcedureName = Text.readString(in);
    if (lastProcedureName.equals(NEXT_PROCEDURE_NONE)) {
      lastProcedure = null;
    } else {
      lastProcedure = procedureTable.get(lastProcedureName);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    BalanceJob bj = (BalanceJob) obj;
    return new EqualsBuilder()
        .append(id, bj.id)
        .append(procedureTable, bj.procedureTable)
        .append(firstProcedure, bj.firstProcedure)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(id)
        .append(procedureTable)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "{jobId=" + id + "}";
  }

  /**
   * Get the detail description of this job.
   */
  public String getDetailMessage() {
    StringBuilder builder = new StringBuilder();
    builder.append("id=").append(id);
    if (firstProcedure != null) {
      builder.append(",firstProcedure=").append(firstProcedure);
    }
    if (curProcedure != null) {
      builder.append(",currentProcedure=").append(curProcedure);
    }
    builder.append(",jobDone=").append(jobDone);
    if (error != null) {
      builder.append(",error=").append(error.getMessage());
    }
    return builder.toString();
  }

  boolean isSchedulerShutdown() {
    return !scheduler.isRunning();
  }

  @VisibleForTesting
  Map<String, T> getProcedureTable() {
    return procedureTable;
  }

  @VisibleForTesting
  T getCurProcedure() {
    return curProcedure;
  }
}