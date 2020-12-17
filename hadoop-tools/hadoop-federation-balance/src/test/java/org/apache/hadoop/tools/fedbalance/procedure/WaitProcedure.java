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

import org.apache.hadoop.util.Time;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This procedure waits specified period of time then finish. It simulates the
 * behaviour of blocking procedures.
 */
public class WaitProcedure extends BalanceProcedure {

  private long waitTime;
  private boolean executed = false;

  public WaitProcedure() {
  }

  public WaitProcedure(String name, long delay, long waitTime) {
    super(name, delay);
    this.waitTime = waitTime;
  }

  @Override
  public boolean execute() throws IOException {
    long startTime = Time.monotonicNow();
    long timeLeft = waitTime;
    while (timeLeft > 0) {
      try {
        Thread.sleep(timeLeft);
      } catch (InterruptedException e) {
        if (isSchedulerShutdown()) {
          return false;
        }
      } finally {
        timeLeft = waitTime - (Time.monotonicNow() - startTime);
      }
    }
    executed = true;
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(waitTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    waitTime = in.readLong();
  }

  public boolean getExecuted() {
    return executed;
  }
}
