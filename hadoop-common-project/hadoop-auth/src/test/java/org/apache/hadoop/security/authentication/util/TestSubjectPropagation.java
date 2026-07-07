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

package org.apache.hadoop.security.authentication.util;

import org.junit.Test;
import org.junit.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;

/**
 * Verifies the JDK22+ Subject-propagation cascade restored by the
 * {@link InheritableThreadLocal}-based mechanism in
 * {@link SubjectUtil#callAs(Subject, java.util.concurrent.Callable)}.
 *
 * <p>The mechanism relies solely on Hadoop's own InheritableThreadLocal layer and
 * the JVM's standard {@code Thread.<init>}-time {@code InheritableThreadLocal} copy.
 * No special {@code Thread} subclass is required: any platform thread ({@link Thread},
 * {@link java.util.concurrent.ForkJoinWorkerThread}, Netty's
 * {@code FastThreadLocalThread}, …) constructed inside a {@code SubjectUtil.callAs}
 * scope inherits the active Subject and observes it via {@link SubjectUtil#current()}.
 */
public class TestSubjectPropagation {

  /** Plain Thread inside callAs sees parent's Subject via SubjectUtil.current(). */
  @Test
  public void testPlainThreadInheritsSubjectViaSubjectUtilCallAs() {
    Subject parent = new Subject();
    AtomicReference<Subject> seen = new AtomicReference<>();
    SubjectUtil.callAs(parent, () -> {
      Thread t = new Thread(() -> seen.set(SubjectUtil.current()), "plain-child");
      t.start();
      t.join(50000);
      return null;
    });
    Assert.assertEquals(parent, seen.get());
  }

  /**
   * Plain Thread submitted to a {@link java.util.concurrent.ThreadPoolExecutor} inside callAs
   * sees parent's Subject. Mimics the ubiquitous Spark / HiveServer2 / generic long-running JVM
   * pattern of submitting work into a long-lived pool from inside a UGI {@code doAs} scope.
   */
  @Test
  public void testPlainThreadInThreadPoolExecutorInheritsSubject() throws Exception {
    Subject parent = new Subject();
    AtomicReference<Subject> seen = new AtomicReference<>();
    ExecutorService pool = Executors.newFixedThreadPool(2, r -> new Thread(r, "plain-pool"));
    try {
      SubjectUtil.callAs(parent, () -> {
        pool.submit(() -> seen.set(SubjectUtil.current()))
            .get(5, TimeUnit.SECONDS);
        return null;
      });
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
    Assert.assertEquals(parent, seen.get());
  }

  /**
   * Transitive cascade: pool worker created inside callAs scope inherits the Subject
   * permanently; even after the original callAs scope exits, the worker still sees the
   * Subject and propagates it to any child Thread it creates. Matches pre-JDK22
   * {@code inheritedAccessControlContext} cascading semantics.
   */
  @Test
  public void testTransitiveCascadeViaPlainPoolAfterCallAsExits() throws Exception {
    Subject parent = new Subject();
    AtomicReference<Subject> seen = new AtomicReference<>();
    ExecutorService pool = Executors.newFixedThreadPool(1, r -> new Thread(r, "plain-pool"));
    try {
      // Step 1: create the worker INSIDE the callAs scope so it inherits the Subject's ThreadLocal.
      SubjectUtil.callAs(parent, () -> {
        pool.submit(() -> { /* warm worker, no-op */ }).get(5, TimeUnit.SECONDS);
        return null;
      });
      // Step 2: callAs has exited. Submit a task that itself spawns a grandchild Thread,
      // OUTSIDE any callAs scope. The grandchild must still see the parent's Subject — this
      // is the "permanent inherited context" semantic that pre-JDK22 had via
      // inheritedAccessControlContext.
      pool.submit(() -> {
        Thread grandchild = new Thread(() -> seen.set(SubjectUtil.current()), "plain-grandchild");
        grandchild.start();
        try {
          grandchild.join(5_000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }).get(5, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
    Assert.assertEquals(parent, seen.get());
  }

  @Test
  public void testNestedCallAsRestoresOuterSubject() {
    Subject s1 = new Subject();
    Subject s2 = new Subject();
    AtomicReference<Subject> insideInner = new AtomicReference<>();
    AtomicReference<Subject> insideOuterAfterInner = new AtomicReference<>();
    SubjectUtil.callAs(s1, () -> {
      SubjectUtil.callAs(s2, () -> {
        insideInner.set(SubjectUtil.current());
        return null;
      });
      insideOuterAfterInner.set(SubjectUtil.current());
      return null;
    });
    Assert.assertEquals(s2, insideInner.get());
    Assert.assertEquals(s1, insideOuterAfterInner.get());
  }

  @Test
  public void testCallAsClearsTlStateOnExit() {
    Subject s = new Subject();
    SubjectUtil.callAs(s, () -> null);
    Assert.assertNull(SubjectUtil.current());
  }

  @Test
  public void testCallAsWithNullSubject() {
    Subject parent = new Subject();
    AtomicReference<Subject> seen = new AtomicReference<>();
    SubjectUtil.callAs(parent, () -> {
      // Nested callAs(null, ...) must produce a null current Subject inside the inner action.
      SubjectUtil.callAs(null, () -> {
        seen.set(SubjectUtil.current());
        return null;
      });
      return null;
    });
    Assert.assertNull(seen.get());
  }

  @Test
  public void testCallAsWithSameSubjectIsNoOpInTlState() {
    // Verify nesting with the same Subject doesn't disturb the prev/restore logic.
    Subject s = new Subject();
    AtomicReference<Subject> after = new AtomicReference<>();
    SubjectUtil.callAs(s, () -> {
      SubjectUtil.callAs(s, () -> null);
      after.set(SubjectUtil.current());
      return null;
    });
    Assert.assertEquals(s, after.get());
  }
}
