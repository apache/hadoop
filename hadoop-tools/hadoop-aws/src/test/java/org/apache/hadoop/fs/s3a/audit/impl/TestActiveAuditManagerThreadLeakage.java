/*
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

package org.apache.hadoop.fs.s3a.audit.impl;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.audit.MemoryHungryAuditor;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_SERVICE_CLASSNAME;
import static org.apache.hadoop.fs.s3a.audit.impl.ActiveAuditManagerS3A.PRUNE_THRESHOLD;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatisticsStore;

/**
 * This test attempts to recreate the OOM problems of
 * HADOOP-18091. S3A auditing leaks memory through ThreadLocal references
 * it does this by creating a thread pool, then
 * creates and destroys FS instances, with threads in
 * the pool (but not the main JUnit test thread) creating
 * audit spans.
 *
 * With a custom audit span with a large memory footprint,
 * memory demands will be high, and if the closed instances
 * don't get cleaned up, memory runs out.
 * GCs are forced.
 * It is critical no spans are created in the junit thread because they will
 * last for the duration of the test JVM.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestActiveAuditManagerThreadLeakage extends AbstractHadoopTestBase {
  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestActiveAuditManagerThreadLeakage.class);

  /** how many managers to sequentially create. */
  private static final int MANAGER_COUNT = 500;

  /** size of long lived thread pool. */
  private static final int THREAD_COUNT = 20;
  private ExecutorService workers;

  /**
   * count of prunings which have taken place in the manager lifecycle
   * operations.
   */
  private int pruneCount;

  /**
   * As audit managers are created they are added to the list,
   * so we can verify they get GC'd.
   */
  private final List<WeakReference<ActiveAuditManagerS3A>> auditManagers =
      new ArrayList<>();

  @After
  public void teardown() {
    if (workers != null) {
      workers.shutdown();
    }
  }


  /**
   * When the service is stopped, the span map is
   * cleared immediately.
   */
  @Test
  public void testSpanMapClearedInServiceStop() throws IOException {
    try (ActiveAuditManagerS3A auditManager =
             new ActiveAuditManagerS3A(emptyStatisticsStore())) {
      auditManager.init(createMemoryHungryConfiguration());
      auditManager.start();
      auditManager.getActiveAuditSpan();
      // get the span map
      final WeakReferenceThreadMap<?> spanMap
          = auditManager.getActiveSpanMap();
      Assertions.assertThat(spanMap.size())
          .describedAs("map size")
          .isEqualTo(1);
      auditManager.stop();
      Assertions.assertThat(spanMap.size())
          .describedAs("map size")
          .isEqualTo(0);
    }
  }

  @Test
  public void testMemoryLeak() throws Throwable {
    workers = Executors.newFixedThreadPool(THREAD_COUNT);
    for (int i = 0; i < MANAGER_COUNT; i++) {
      final long oneAuditConsumption = createAndTestOneAuditor();
      LOG.info("manager {} memory retained {}", i, oneAuditConsumption);
    }

    // pruning must have taken place.
    // that's somewhat implicit in the test not going OOM.
    // but if memory allocation in test runs increase, it
    // may cease to hold. in which case: create more
    // audit managers
    LOG.info("Total prune count {}", pruneCount);

    Assertions.assertThat(pruneCount)
        .describedAs("Total prune count")
        .isNotZero();

    // now count number of audit managers GC'd
    // some must have been GC'd, showing that no other
    // references are being retained internally.
    Assertions.assertThat(auditManagers.stream()
            .filter((r) -> r.get() == null)
            .count())
        .describedAs("number of audit managers garbage collected")
        .isNotZero();
  }

  /**
   * Create, use and then shutdown one auditor in a unique thread.
   * @return memory consumed/released
   */
  private long createAndTestOneAuditor() throws Exception {
    long original = Runtime.getRuntime().freeMemory();
    ExecutorService factory = Executors.newSingleThreadExecutor();

    try {
      pruneCount += factory.submit(this::createAuditorAndWorkers).get();
    } finally {
      factory.shutdown();
      factory.awaitTermination(60, TimeUnit.SECONDS);
    }


    final long current = Runtime.getRuntime().freeMemory();
    return current - original;

  }

  /**
   * This is the core of the leakage test.
   * Create an audit manager and spans across multiple threads.
   * The spans are created in the long-lived pool, so if there is
   * any bonding of the life of managers/spans to that of threads,
   * it will surface as OOM events.
   * @return count of weak references whose reference values were
   * nullified.
   */
  private int createAuditorAndWorkers()
      throws IOException, InterruptedException, ExecutionException {
    try (ActiveAuditManagerS3A auditManager =
             new ActiveAuditManagerS3A(emptyStatisticsStore())) {
      auditManager.init(createMemoryHungryConfiguration());
      auditManager.start();
      LOG.info("Using {}", auditManager);
      auditManagers.add(new WeakReference<>(auditManager));

      // no guarantee every thread gets used, so track
      // in a set. This will give us the thread ID of every
      // entry in the map.

      Set<Long> threadIds = new HashSet<>();

      List<Future<Result>> futures = new ArrayList<>(THREAD_COUNT);

      // perform the spanning operation in a long lived thread.
      for (int i = 0; i < THREAD_COUNT; i++) {
        futures.add(workers.submit(() -> spanningOperation(auditManager)));
      }

      // get the results and so determine the thread IDs
      for (Future<Result> future : futures) {
        final Result r = future.get();
        threadIds.add(r.getThreadId());
      }

      final int threadsUsed = threadIds.size();
      final Long[] threadIdArray = threadIds.toArray(new Long[0]);

      // gc
      System.gc();
      // get the span map
      final WeakReferenceThreadMap<?> spanMap
          = auditManager.getActiveSpanMap();

      // count number of spans removed
      final long derefenced = threadIds.stream()
          .filter((id) -> !spanMap.containsKey(id))
          .count();
      if (derefenced > 0) {
        LOG.info("{} executed across {} threads and dereferenced {} entries",
            auditManager, threadsUsed, derefenced);
      }

      // resolve not quite all of the threads.
      // why not all? leaves at least one for pruning
      // but it does complicate some of the assertions...
      int spansRecreated = 0;
      int subset = threadIdArray.length - 1;
      LOG.info("Resolving {} thread references", subset);
      for (int i = 0; i < subset; i++) {
        final long id = threadIdArray[i];

        // note whether or not the span is present
        final boolean present = spanMap.containsKey(id);

        // get the the span for that ID. which must never be
        // null
        Assertions.assertThat(spanMap.get(id))
            .describedAs("Span map entry for thread %d", id)
            .isNotNull();

        // if it wasn't present, the unbounded span must therefore have been
        // bounded to this map entry.
        if (!present) {
          spansRecreated++;
        }
      }
      LOG.info("Recreated {} spans", subset);

      // if the number of spans lost is more than the number
      // of entries not probed, then at least one span was
      // recreated
      if (derefenced > threadIdArray.length - subset) {
        Assertions.assertThat(spansRecreated)
            .describedAs("number of recreated spans")
            .isGreaterThan(0);
      }

      // now prune.
      int pruned = auditManager.prune();
      if (pruned > 0) {
        LOG.info("{} executed across {} threads and pruned {} entries",
            auditManager, threadsUsed, pruned);
      }
      Assertions.assertThat(pruned)
          .describedAs("Count of references pruned")
          .isEqualTo(derefenced - spansRecreated);
      return pruned + (int) derefenced;
    }

  }

  private Configuration createMemoryHungryConfiguration() {
    final Configuration conf = new Configuration(false);
    conf.set(AUDIT_SERVICE_CLASSNAME, MemoryHungryAuditor.NAME);
    return conf;
  }

  /**
   * The operation in each worker thread.
   * @param auditManager audit manager
   * @return result of the call
   * @throws IOException troluble
   */
  private Result spanningOperation(final ActiveAuditManagerS3A auditManager)
      throws IOException {
    auditManager.getActiveAuditSpan();
    final AuditSpanS3A auditSpan =
        auditManager.createSpan("span", null, null);
    Assertions.assertThat(auditSpan)
        .describedAs("audit span for current thread")
        .isNotNull();
    // this is needed to ensure that more of the thread pool is used up
    Thread.yield();
    return new Result(Thread.currentThread().getId());
  }

  /**
   * Result of the spanning operation.
   */
  private static final class Result {
    /** thread operation took place in. */
    private final long threadId;


    private Result(final long threadId) {
      this.threadId = threadId;
    }

    private long getThreadId() {
      return threadId;
    }


  }

  /**
   * Verify that pruning takes place intermittently.
   */
  @Test
  public void testRegularPruning() throws Throwable {
    try (ActiveAuditManagerS3A auditManager =
             new ActiveAuditManagerS3A(emptyStatisticsStore())) {
      auditManager.init(createMemoryHungryConfiguration());
      auditManager.start();
      // get the span map
      final WeakReferenceThreadMap<?> spanMap
          = auditManager.getActiveSpanMap();
      // add a null entry at a thread ID other than this one
      spanMap.put(Thread.currentThread().getId() + 1, null);

      // remove this span enough times that pruning shall take
      // place twice
      // this verifies that pruning takes place and that the
      // counter is reset
      int pruningCount = 0;
      for (int i = 0; i < PRUNE_THRESHOLD * 2 + 1; i++) {
        boolean pruned = auditManager.removeActiveSpanFromMap();
        if (pruned) {
          pruningCount++;
        }
      }
      // pruning must have taken place
      Assertions.assertThat(pruningCount)
          .describedAs("Intermittent pruning count")
          .isEqualTo(2);
    }
  }

  /**
   * Verify span deactivation removes the entry from the map.
   */
  @Test
  public void testSpanDeactivationRemovesEntryFromMap() throws Throwable {
    try (ActiveAuditManagerS3A auditManager =
             new ActiveAuditManagerS3A(emptyStatisticsStore())) {
      auditManager.init(createMemoryHungryConfiguration());
      auditManager.start();
      // get the span map
      final WeakReferenceThreadMap<?> spanMap
          = auditManager.getActiveSpanMap();
      final AuditSpanS3A auditSpan =
          auditManager.createSpan("span", null, null);
      Assertions.assertThat(auditManager.getActiveAuditSpan())
          .describedAs("active span")
          .isSameAs(auditSpan);
      // this assert gets used repeatedly, so define a lambda-exp
      // which can be envoked with different arguments
      Consumer<Boolean> assertMapHasKey = expected ->
          Assertions.assertThat(spanMap.containsKey(spanMap.currentThreadId()))
              .describedAs("map entry for current thread")
              .isEqualTo(expected);

      // sets the span to null
      auditSpan.deactivate();

      // there's no entry
      assertMapHasKey.accept(false);

      // asking for the current span will return the unbonded one
      final AuditSpanS3A newSpan = auditManager.getActiveAuditSpan();
      Assertions.assertThat(newSpan)
          .describedAs("active span")
          .isNotNull()
          .matches(s -> !s.isValidSpan());
      // which is in the map
      // there's an entry
      assertMapHasKey.accept(true);

      // deactivating the old span does nothing
      auditSpan.deactivate();
      assertMapHasKey.accept(true);

      // deactivating the current unbounded span does
      // remove the entry
      newSpan.deactivate();
      assertMapHasKey.accept(false);
    }
  }
}
