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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

  @Test
  public void testMemoryLeak() throws Throwable {
    workers = Executors.newFixedThreadPool(THREAD_COUNT);
    for (int i = 0; i < MANAGER_COUNT; i++) {
      final long oneAuditConsumption = createDestroyOneAuditor();
      LOG.info("manager {} memory retained {}", i, oneAuditConsumption);
    }
  }

  private long createDestroyOneAuditor() throws Exception {
    long original = Runtime.getRuntime().freeMemory();
    ExecutorService factory = Executors.newSingleThreadExecutor();

    final Future<Long> action = factory.submit(this::createAuditorAndWorkers);
    final long count = action.get();
    LOG.info("Span count {}", count);

    factory.shutdown();
    factory.awaitTermination(60, TimeUnit.SECONDS);

    System.gc();
    final long current = Runtime.getRuntime().freeMemory();
    return current - original;

  }

  private long createAuditorAndWorkers()
      throws IOException, InterruptedException, ExecutionException {
    final Configuration conf = new Configuration(false);
    conf.set(AUDIT_SERVICE_CLASSNAME, MemoryHungryAuditor.NAME);
    try (ActiveAuditManagerS3A auditManager = new ActiveAuditManagerS3A(emptyStatisticsStore())){
      auditManager.init(conf);
      auditManager.start();
      LOG.info("Using {}", auditManager);
      // no guarentee every thread gets used

      List<Future<AuditSpanS3A>> futures = new ArrayList<>(THREAD_COUNT);
      for (int i = 0; i < THREAD_COUNT; i++) {
        futures.add(workers.submit(() -> worker(auditManager)));
      }

      for (Future<AuditSpanS3A> future : futures) {
        future.get();
      }
      return ((MemoryHungryAuditor)auditManager.getAuditor()).getSpanCount();
    }

  }

  private AuditSpanS3A worker(final ActiveAuditManagerS3A auditManager) throws IOException {
    return auditManager.createSpan("span", null, null);
  }
}
