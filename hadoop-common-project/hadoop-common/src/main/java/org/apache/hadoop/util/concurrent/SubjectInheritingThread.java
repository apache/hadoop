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

package org.apache.hadoop.util.concurrent;

import java.security.PrivilegedAction;
import javax.security.auth.Subject;

import org.apache.hadoop.security.authentication.util.SubjectUtil;

/**
 * Helper class to restore Subject propagation behavior of threads after the
 * JEP411/JEP486 changes.
 * <p>
 * Java propagates the current Subject to any new Threads in all version up to
 * Java 21. In Java 22-23 the Subject is only propagated if the SecurityManager
 * is enabled, while in Java 24+ it is never propagated.
 * <p>
 * Hadoop security heavily relies on the original behavior, as Subject is at the
 * core of JAAS. This class wraps thread. It overrides start() and saves the
 * Subject of the current thread, and wraps the payload in a
 * Subject.doAs()/callAs() call to restore it in the newly created Thread.
 * <p>
 * When specifying a Runnable, this class is used in exactly the same way as
 * Thread.
 * <p>
 * {@link #run()} cannot be directly overridden, as that would also override the
 * subject restoration logic. SubjectInheritingThread provides a {@link #work()}
 * method instead, which is wrapped and invoked by its own final {@link #run()}
 * method.
 */
public class SubjectInheritingThread extends Thread {

  private Subject startSubject;
  // {@link Thread#target} is private, so we need our own
  private Runnable hadoopTarget;

  /**
   * Behaves similarly to {@link Thread#Thread()} constructor, but the code to run
   * must be specified by overriding the {@link #work()} instead of the {link
   * #run()} method.
   */
  public SubjectInheritingThread() {
    super();
  }

  /**
   * Behaves similarly to {@link Thread#Thread(Runnable)} constructor.
   *
   * @param target the object whose {@code run} method is invoked when this thread
   *               is started. If {@code null}, this classes {@code run} method
   *               does nothing.
   */
  public SubjectInheritingThread(Runnable target) {
    super();
    this.hadoopTarget = target;
  }

  /**
   * Behaves similarly to {@link Thread#Thread(ThreadGroup, Runnable)}
   * constructor.
   *
   * @param group  the thread group. If {@code null} and there is a security
   *               manager, the group is determined by
   *               {@linkplain SecurityManager#getThreadGroup
   *               SecurityManager.getThreadGroup()}. If there is not a security
   *               manager or {@code
   *         SecurityManager.getThreadGroup()} returns {@code null}, the group is
   *               set to the current thread's thread group.
   *
   * @param target the object whose {@code run} method is invoked when this thread
   *               is started. If {@code null}, this thread's run method is
   *               invoked.
   * @throws SecurityException if the current thread cannot create a thread in the
   *                           specified thread group
   */
  public SubjectInheritingThread(ThreadGroup group, Runnable target) {
    // The target passed to Thread has no effect, we only pass it
    // because there is no super(group) constructor.
    super(group, target);
    this.hadoopTarget = target;
  }

  /**
   * Behaves similarly to {@link Thread#Thread(Runnable, String)} constructor.
   *
   * @param target the object whose {@code run} method is invoked when this thread
   *               is started. If {@code null}, this thread's run method is
   *               invoked.
   *
   * @param name   the name of the new thread
   *
   * @throws SecurityException if the current thread cannot create a thread in the
   *                           specified thread group
   */
  public SubjectInheritingThread(Runnable target, String name) {
    super(name);
    this.hadoopTarget = target;
  }

  /**
   * Behaves similarly to {@link Thread#Thread(String)} constructor.
   *
   * @param name the name of the new thread
   */
  public SubjectInheritingThread(String name) {
    super(name);
  }

  /**
   * Behaves similarly to {@link Thread#Thread(ThreadGroup, String)} constructor.
   *
   * @param group the thread group. If {@code null} and there is a security
   *              manager, the group is determined by
   *              {@linkplain SecurityManager#getThreadGroup
   *              SecurityManager.getThreadGroup()}. If there is not a security
   *              manager or {@code
   *         SecurityManager.getThreadGroup()} returns {@code null}, the group is
   *              set to the current thread's thread group.
   *
   * @param name  the name of the new thread
   */
  public SubjectInheritingThread(ThreadGroup group, String name) {
    super(group, name);
  }

  /**
   * Behaves similarly to {@link Thread#Thread(ThreadGroup, Runnable, String)}
   * constructor.
   *
   * @param group  the thread group. If {@code null} and there is a security
   *               manager, the group is determined by
   *               {@linkplain SecurityManager#getThreadGroup
   *               SecurityManager.getThreadGroup()}. If there is not a security
   *               manager or {@code
   *         SecurityManager.getThreadGroup()} returns {@code null}, the group is
   *               set to the current thread's thread group.
   *
   * @param target the object whose {@code run} method is invoked when this thread
   *               is started. If {@code null}, this thread's run method is
   *               invoked.
   *
   * @param name   the name of the new thread
   *
   * @throws SecurityException if the current thread cannot create a thread in the
   *                           specified thread group or cannot override the
   *                           context class loader methods.
   */
  public SubjectInheritingThread(ThreadGroup group, Runnable target, String name) {
    super(group, name);
    this.hadoopTarget = target;
  }

  /**
   * Behaves similarly to pre-Java 22 {@link Thread#start()}. It saves the current
   * Subject before starting the new thread, which is then used as the Subject for
   * the Runnable or the overridden work() method.
   */
  @Override
  public final void start() {
    if (!SubjectUtil.THREAD_INHERITS_SUBJECT) {
      startSubject = SubjectUtil.current();
    }
    super.start();
  }

  /**
   * This is the equivalent of {@link Thread#run()}. Override this instead of
   * {@link #run()} Subject will be propagated like in pre-Java 22 Thread.
   */
  public void work() {
    if (hadoopTarget != null) {
      hadoopTarget.run();
    }
  }

  /**
   * This cannot be overridden in this class. Override the {@link #work()} method
   * instead which behaves like pre-Java 22 {@link Thread#run()}
   */
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
}
