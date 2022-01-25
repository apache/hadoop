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

package org.apache.hadoop.fs.s3a.audit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.ActiveAuditManagerS3A;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_SERVICE_CLASSNAME;
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
 * It is critical no spans are created in the junit thread because they will last for
 * the duration of the test JVM.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestActiveAuditManagerThreadLeakage extends AbstractHadoopTestBase {
  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestActiveAuditManagerThreadLeakage.class);

  private static final int MANAGER_COUNT = 1000;
  private static final int THREAD_COUNT = 20;
  private ExecutorService workers;
  private int pruneCount;

  @Test
  public void testMemoryLeak() throws Throwable {
    workers = Executors.newFixedThreadPool(THREAD_COUNT);
    for (int i = 0; i < MANAGER_COUNT; i++) {
      final long oneAuditConsumption = createDestroyOneAuditor(i);
      LOG.info("manager {} memory retained {}", i, oneAuditConsumption);
    }

    // pruning must have taken place.
    // that's somewhat implicit in the test not going OOM.
    // but if memory allocation in test runs increase, it
    // may cease to hold. in which case: create more
    // audit managers
    LOG.info("Totel prune count {}", pruneCount);

    Assertions.assertThat(pruneCount)
        .describedAs("Totel prune count")
        .isGreaterThan(0);


  }

  private long createDestroyOneAuditor(final int instance) throws Exception {
    long original = Runtime.getRuntime().freeMemory();
    ExecutorService factory = Executors.newSingleThreadExecutor();

    final Future<Integer> action = factory.submit(this::createAuditorAndWorkers);
    final int pruned = action.get();
    pruneCount += pruned;

    factory.shutdown();
    factory.awaitTermination(60, TimeUnit.SECONDS);


    final long current = Runtime.getRuntime().freeMemory();
    return current - original;

  }

  private int createAuditorAndWorkers()
      throws IOException, InterruptedException, ExecutionException {
    final Configuration conf = new Configuration(false);
    conf.set(AUDIT_SERVICE_CLASSNAME, MemoryHungryAuditor.NAME);
    try (ActiveAuditManagerS3A auditManager = new ActiveAuditManagerS3A(emptyStatisticsStore())){
      auditManager.init(conf);
      auditManager.start();
      LOG.info("Using {}", auditManager);
      // no guarentee every thread gets used

      List<Future<Result>> futures = new ArrayList<>(THREAD_COUNT);
      Set<Long> threadIds = new HashSet<>();
      for (int i = 0; i < THREAD_COUNT; i++) {
        futures.add(workers.submit(() -> spanningOperation(auditManager)));
      }

      for (Future<Result> future : futures) {
        final Result r = future.get();
        threadIds.add(r.getThreadId());
      }
      // get rid of any references to spans other than in the weak ref map
      futures = null;
      // gc
      System.gc();
      final int pruned = auditManager.prune();
      LOG.info("{} executed across {} threads and pruned {} entries",
          auditManager, threadIds.size(), pruned);
      return pruned;
    }

  }

  private Result spanningOperation(final ActiveAuditManagerS3A auditManager)
      throws IOException, InterruptedException {
    final AuditSpanS3A auditSpan = auditManager.getActiveAuditSpan();
    Assertions.assertThat(auditSpan)
        .describedAs("audit span for current thread")
        .isNotNull();
    Thread.sleep(200);
    return new Result(Thread.currentThread().getId(), auditSpan);
  }

  private static final class Result {
    private final long threadId;
    private final AuditSpanS3A auditSpan;

    public Result(final long threadId, final AuditSpanS3A auditSpan) {
      this.threadId = threadId;
      this.auditSpan = auditSpan;
    }

    public long getThreadId() {
      return threadId;
    }

    public AuditSpanS3A getAuditSpan() {
      return auditSpan;
    }
  }
}
