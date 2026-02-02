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

/**
 * A thread that has called {@link Thread#setDaemon(boolean) } with true.
 * <p>
 * The runnable code must either be specified in the runnable parameter or in
 * the overridden work() method.
 * <p>
 * See {@link org.apache.hadoop.util.concurrent.SubjectInheritingThread} for the Subject inheritance behavior this
 * class adds.
 *
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
@InterfaceStability.Unstable
public class Daemon extends Thread {

  Subject startSubject;

  @Override
  public final void start() {
    if (!SubjectUtil.THREAD_INHERITS_SUBJECT) {
      startSubject = SubjectUtil.current();
    }
    super.start();
  }

  /**
   * Override this instead of run()
   */
  public void work() {
    if (runnable != null) {
      runnable.run();
    }
  }

  @Override
  public final void run() {
    if (!SubjectUtil.THREAD_INHERITS_SUBJECT) {
      SubjectUtil.doAs(startSubject, new PrivilegedAction<Void>() {

        @Override
        public Void run() {
          work();
          return null;
        }

      });
    } else {
      work();
    }
  }

  {
    setDaemon(true); // always a daemon
  }

  /**
   * Provide a factory for named daemon threads, for use in ExecutorServices
   * constructors
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
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
   *
   * @param runnable runnable.
   */
  public Daemon(Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((Object) runnable).toString());
  }

  /**
   * Construct a daemon thread to be part of a specified thread group.
   *
   * @param group    thread group.
   * @param runnable runnable.
   */
  public Daemon(ThreadGroup group, Runnable runnable) {
    super(group, runnable);
    this.runnable = runnable;
    this.setName(((Object) runnable).toString());
  }

  public Runnable getRunnable() {
    return runnable;
  }
}
