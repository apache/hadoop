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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.tools.fedbalance.procedure.BalanceJob.NEXT_PROCEDURE_NONE;

/**
 * The basic components of the Job. Extend this class to implement different
 * job logic.
 */
public abstract class BalanceProcedure<T extends BalanceProcedure>
    implements Writable {

  public static final Logger LOG =
      LoggerFactory.getLogger(BalanceProcedure.class);
  private String nextProcedure; // the procedure after this procedure.
  private String name; // the name of this procedure.
  private long delayDuration; // this specifies how long will this procedure be
                              // delayed. The delay is triggered by throwing a
                              // RetryException.
  private BalanceJob job;

  public BalanceProcedure() {
  }

  /**
   * The constructor of BalanceProcedure.
   *
   * @param name the name of the procedure.
   * @param nextProcedure the name of the next procedure.
   * @param delayDuration the delay duration when this procedure is delayed.
   */
  public BalanceProcedure(String name, String nextProcedure,
      long delayDuration) {
    this();
    this.name = name;
    this.nextProcedure = nextProcedure;
    this.delayDuration = delayDuration;
  }

  public BalanceProcedure(String name, long delayDuration) {
    this(name, NEXT_PROCEDURE_NONE, delayDuration);
  }

  /**
   * The main process. This is called by the ProcedureScheduler.

   * Make sure the process quits fast when it's interrupted and the scheduler is
   * shut down.
   *
   * One procedure may have many phases and all the phases share the same member
   * variables. Each time this method returns, the journal is saved. User can
   * serialize the current phase in write(DataOutput) so the job can continue
   * with the last unfinished phase after it is recovered.
   * The return value indicates whether the job should go to the next procedure.
   * Return true after all the phases finish.
   *
   * Example:
   * class ProcedureWithManyPhase extends BalanceProcedure {
   *
   *   enum PHASE {
   *     P1, P2, P3
   *   }
   *   PHASE phase;
   *
   *   public boolean execute(T lastProcedure) throws RetryException,
   *     IOException {
   *     switch (phase) {
   *     case P1:
   *       // do something.
   *       return false;
   *     case P2:
   *       // do something.
   *       return false;
   *     case P3:
   *       // do something.
   *       return true;
   *     default:
   *       throw new IOException("Unexpected phase " + phase);
   *     }
   *   }
   *
   *   public void write(DataOutput out) {
   *     out.writeInt(phase.ordinal());
   *   }
   *
   *   public void readFields(DataInput in) throws IOException {
   *     stage = Stage.values()[in.readInt()];
   *   }
   * }
   *
   *
   * @throws RetryException if this procedure needs delay a while then retry.
   * @return true if the procedure has done and the job will go to the next
   *         procedure, otherwise false.
   */
  public abstract boolean execute() throws RetryException, IOException;

  /**
   * The time in milliseconds the procedure should wait before retry.
   */
  public long delayMillisBeforeRetry() {
    return delayDuration;
  }

  /**
   * The active flag.
   */
  protected boolean isSchedulerShutdown() {
    return job.isSchedulerShutdown();
  }

  protected void setNextProcedure(String nextProcedure) {
    this.nextProcedure = nextProcedure;
  }

  void setJob(BalanceJob job) {
    this.job = job;
  }

  /**
   * Get the next procedure.
   */
  public String nextProcedure() {
    return nextProcedure;
  }

  /**
   * Get the procedure name.
   */
  public String name() {
    return name;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (nextProcedure == null) {
      Text.writeString(out, NEXT_PROCEDURE_NONE);
    } else {
      Text.writeString(out, nextProcedure);
    }
    Text.writeString(out, name);
    new LongWritable(delayDuration).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    nextProcedure = Text.readString(in);
    name = Text.readString(in);
    delayDuration = readLong(in);
  }

  private static long readLong(DataInput in) throws IOException {
    LongWritable delayWritable = new LongWritable();
    delayWritable.readFields(in);
    return delayWritable.get();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(nextProcedure)
        .append(name)
        .append(delayDuration)
        .toHashCode();
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
    BalanceProcedure rhs = (BalanceProcedure) obj;
    return new EqualsBuilder()
        .append(nextProcedure, rhs.nextProcedure)
        .append(name, rhs.name)
        .append(delayDuration, rhs.delayDuration)
        .build();
  }

  @Override
  public String toString() {
    return name + ":" + this.getClass().getName();
  }

  /**
   * The RetryException represents the current procedure should be delayed then
   * retried.
   */
  public static class RetryException extends Exception {
    public RetryException() {}
  }
}
