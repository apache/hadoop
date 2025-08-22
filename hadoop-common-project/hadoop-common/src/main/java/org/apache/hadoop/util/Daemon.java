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

package org.apache.hadoop.util;

import java.security.PrivilegedAction;
import java.util.concurrent.ThreadFactory;

import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.util.SubjectUtil;

/** A thread that has called {@link Thread#setDaemon(boolean) } with true.
 * 
 * The runnable code must either be specified in the runnable parameter or
 * in the override work() method. 
 * 
 * The subject propagation is already added in either case. 
 * 
 * */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class Daemon extends Thread {

  Subject startSubject;
  
//To be replaced with this in the next JDK24 patch:
 @Override
 public final void start() {
   startSubject = SubjectUtil.current();
   super.start();
 }
  
  /**
   * Override this instead of run()
   */
  public void work() {
    throw new IllegalArgumentException("");
  }
  
  @Override
  public final void run() {
      SubjectUtil.doAs(startSubject, new PrivilegedAction<Void>() {

        @Override
        public Void run() {
          if (runnable != null) {
            runnable.run();
          } else {
            work();
          }
          return null;
        }

      });
  }
  
  {
    setDaemon(true);                              // always a daemon
  }

  /**
   * Provide a factory for named daemon threads,
   * for use in ExecutorServices constructors
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public static class DaemonFactory extends Daemon implements ThreadFactory {

    @Override
    public Thread newThread(Runnable runnable) {
      return new Daemon(runnable);
    }

  }

  Runnable runnable = null;
  /** Construct a daemon thread. */
  public Daemon() {
    super();
  }

  /**
   * Construct a daemon thread.
   * @param runnable runnable.
   */
  public Daemon(Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((Object)runnable).toString());
  }

  /**
   * Construct a daemon thread to be part of a specified thread group.
   * @param group thread group.
   * @param runnable runnable.
   */
  public Daemon(ThreadGroup group, Runnable runnable) {
    super(group, runnable);
    this.runnable = runnable;
    this.setName(((Object)runnable).toString());
  }

  public Runnable getRunnable() {
    return runnable;
  }
}
