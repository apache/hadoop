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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

/**
 * Helper class to restore Subject propagation behavior after the JEP411/JEP486
 * changes
 * 
 * Runnables can be specified normally, but the work() method has to be
 * overridden instead of run() when subclassing.
 */
public class HadoopThread extends Thread {

  Subject startSubject;
  Runnable hadoopTarget;

  public HadoopThread() {
    super();
  }

  public HadoopThread(Runnable target) {
    super();
    this.hadoopTarget = target;
  }

  public HadoopThread(ThreadGroup group, Runnable target) {
    // The target passed to Thread has no effect, we only pass it
    // because there is no super(group) constructor.
    super(group, target);
    this.hadoopTarget = target;
  }

  public HadoopThread(Runnable target, String name) {
    super(name);
    this.hadoopTarget = target;
  }

  public HadoopThread(String name) {
    super(name);
  }

  public HadoopThread(ThreadGroup group, String name) {
    super(group, name);
  }

  public HadoopThread(ThreadGroup group, Runnable target, String name) {
    super(group, name);
    this.hadoopTarget = target;
  }

  @Override
  public final void start() {
    // This is temporary, to be replaced with JVM version dependent
    // Subject.current() shim
    AccessControlContext context = AccessController.getContext();
    startSubject = Subject.getSubject(context);
    super.start();
  }

// To be replaced with this in the next JDK24 patch:
//  @Override
//  public final void start() {
//    startSubject = SubjectUtil.current();
//    super.start();
//  }

  /**
   * Override this instead of run()
   * 
   * It is really unfortunate that we have to introduce a new method and cannot reuse run(),
   * but since run() is designed to be overridden, I couldn't find any other way to make this work.
   *  
   */
  public void work() {
    throw new IllegalArgumentException("No Runnable was specified and work() is not overriden");
  }

  @Override
  public final void run() {
    try {
      // This is temporary, to be replaced with JVM version dependent shim code
      Subject.doAs(startSubject, new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          if (hadoopTarget != null) {
            hadoopTarget.run();
          } else {
            work();
          }
          return null;
        }

      });
    } catch (PrivilegedActionException ce) {
      Exception t = ce.getException();
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      } else {
        throw new RuntimeException("Unexpected exception", t);
      }
    }
  }

  // To replaced with this in the next patch:
//  /**
//   * Override this instead of run()
//   */
//  public void work() {
//    throw new IllegalArgumentException("No Runnable was specified and work() is not overriden");
//  }
//  
//  @Override
//  public final void run() {
//    try {
//      SubjectUtil.callAs(startSubject, new Callable<Void>() {
//
//        @Override
//        public Void call() throws Exception {
//          if (hadoopTarget != null) {
//            hadoopTarget.run();
//          } else {
//            work();
//          }
//          return null;
//        }
//
//      });
//    } catch (CompletionException ce) {
//      Throwable t = ce.getCause();
//      if (t instanceof RuntimeException) {
//        throw (RuntimeException) t;
//      } else if (t instanceof Error) {
//        throw (Error) t;
//      } else {
//        throw new RuntimeException("Unexpected exception", t);
//      }
//    }
//  }
}
