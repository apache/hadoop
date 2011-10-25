/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Sleeper;

/**
 * Chore is a task performed on a period in hbase.  The chore is run in its own
 * thread. This base abstract class provides while loop and sleeping facility.
 * If an unhandled exception, the threads exit is logged.
 * Implementers just need to add checking if there is work to be done and if
 * so, do it.  Its the base of most of the chore threads in hbase.
 *
 * <p>Don't subclass Chore if the task relies on being woken up for something to
 * do, such as an entry being added to a queue, etc.
 */
public abstract class Chore extends HasThread {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final Sleeper sleeper;
  protected final Stoppable stopper;

  /**
   * @param p Period at which we should run.  Will be adjusted appropriately
   * should we find work and it takes time to complete.
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   * cleanup and exit cleanly.
   */
  public Chore(String name, final int p, final Stoppable stopper) {
    super(name);
    this.sleeper = new Sleeper(p, stopper);
    this.stopper = stopper;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    try {
      boolean initialChoreComplete = false;
      while (!this.stopper.isStopped()) {
        long startTime = System.currentTimeMillis();
        try {
          if (!initialChoreComplete) {
            initialChoreComplete = initialChore();
          } else {
            chore();
          }
        } catch (Exception e) {
          LOG.error("Caught exception", e);
          if (this.stopper.isStopped()) {
            continue;
          }
        }
        this.sleeper.sleep(startTime);
      }
    } catch (Throwable t) {
      LOG.fatal(getName() + "error", t);
    } finally {
      LOG.info(getName() + " exiting");
    }
  }

  /**
   * If the thread is currently sleeping, trigger the core to happen immediately.
   * If it's in the middle of its operation, will begin another operation
   * immediately after finishing this one.
   */
  public void triggerNow() {
    this.sleeper.skipSleepCycle();
  }

  /**
   * Override to run a task before we start looping.
   * @return true if initial chore was successful
   */
  protected boolean initialChore() {
    // Default does nothing.
    return true;
  }

  /**
   * Look for chores.  If any found, do them else just return.
   */
  protected abstract void chore();

  /**
   * Sleep for period.
   */
  protected void sleep() {
    this.sleeper.sleep();
  }
}
