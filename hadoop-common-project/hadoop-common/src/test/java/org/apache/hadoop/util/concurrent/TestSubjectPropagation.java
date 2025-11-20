/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.apache.hadoop.security.authentication.util.SubjectUtil;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Shell;
import org.junit.jupiter.api.Test;

public class TestSubjectPropagation {

  private Subject childSubject = null;

  @Test
  public void testSubjectInheritingThreadOverride() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {
        SubjectInheritingThread t = new SubjectInheritingThread() {
          @Override
          public void work() {
            childSubject = SubjectUtil.current();
          }
        };
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    assertEquals(parentSubject, childSubject);
  }

  @Test
  public void testSubjectInheritingThreadRunnable() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            childSubject = SubjectUtil.current();
          }
        };

        SubjectInheritingThread t = new SubjectInheritingThread(r);
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    assertEquals(parentSubject, childSubject);
  }

  @Test
  public void testDaemonOverride() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Daemon t = new Daemon() {
          @Override
          public void work() {
            childSubject = SubjectUtil.current();
          }
        };
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    assertEquals(parentSubject, childSubject);
  }

  @Test
  public void testDaemonRunnable() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            childSubject = SubjectUtil.current();
          }
        };

        Daemon t = new Daemon(r);
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    assertEquals(parentSubject, childSubject);
  }

  @Test
  public void testThreadOverride() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {

        Thread t = new Thread() {
          @Override
          public void run() {
            childSubject = SubjectUtil.current();
          }
        };
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {

      assertEquals(parentSubject, childSubject);
    } else {
      // This is the behaviour that breaks Hadoop authorization
      // This would fail for Java 22-23 if the SecurityManager would be enabled,
      // but we don't run tests with the SecurityManager enabled.
      assertNull(childSubject);
    }
  }

  @Test
  public void testThreadRunnable() {
    Subject parentSubject = new Subject();
    childSubject = null;

    SubjectUtil.callAs(parentSubject, new Callable<Void>() {
      public Void call() throws InterruptedException {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            childSubject = SubjectUtil.current();
          }
        };

        Thread t = new Thread(r);
        t.start();
        t.join(1000);
        return (Void) null;
      }
    });

    if (SubjectUtil.THREAD_INHERITS_SUBJECT) {
      assertEquals(parentSubject, childSubject);
    } else {
      // This is the behaviour that breaks Hadoop authorization
      // This would fail for Java 22-23 if the SecurityManager would be enabled,
      // but we don't run tests with the SecurityManager enabled.
      assertNull(childSubject);
    }
  }

}
