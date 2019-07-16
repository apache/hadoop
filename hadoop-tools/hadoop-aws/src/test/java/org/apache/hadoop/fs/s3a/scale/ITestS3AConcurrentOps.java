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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils.NanoTimer;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Tests concurrent operations on a single S3AFileSystem instance.
 */
public class ITestS3AConcurrentOps extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AConcurrentOps.class);
  private final int concurrentRenames = 10;
  private final int fileSize = 1024 * 1024;
  private Path testRoot;
  private S3AFileSystem auxFs;

  @Override
  protected int getTestTimeoutSeconds() {
    return 16 * 60;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    auxFs = getNormalFileSystem();

    testRoot = path("/ITestS3AConcurrentOps");
    testRoot = S3ATestUtils.createTestPath(testRoot);
  }

  private S3AFileSystem getNormalFileSystem() throws Exception {
    S3AFileSystem s3a = new S3AFileSystem();
    Configuration conf = createScaleConfiguration();
    URI rootURI = new URI(conf.get(TEST_FS_S3A_NAME));
    s3a.initialize(rootURI, conf);
    return s3a;
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
    if (auxFs != null) {
      auxFs.delete(testRoot, true);
      auxFs.close();
    }
  }

  private void parallelRenames(int concurrentRenames, final S3AFileSystem fs,
      String sourceNameBase, String targetNameBase) throws ExecutionException,
      InterruptedException, IOException {

    Path[] source = new Path[concurrentRenames];
    Path[] target = new Path[concurrentRenames];

    for (int i = 0; i < concurrentRenames; i++){
      source[i] = new Path(testRoot, sourceNameBase + i);
      target[i] = new Path(testRoot, targetNameBase + i);
    }

    LOG.info("Generating data...");
    auxFs.mkdirs(testRoot);
    byte[] zeroes = ContractTestUtils.dataset(fileSize, 0, Integer.MAX_VALUE);
    for (Path aSource : source) {
      try(FSDataOutputStream out = auxFs.create(aSource)) {
        for (int mb = 0; mb < 20; mb++) {
          LOG.debug("{}: Block {}...", aSource, mb);
          out.write(zeroes);
        }
      }
    }
    LOG.info("Data generated...");

    ExecutorService executor = Executors.newFixedThreadPool(
        concurrentRenames, new ThreadFactory() {
          private AtomicInteger count = new AtomicInteger(0);

          public Thread newThread(Runnable r) {
            return new Thread(r,
                "testParallelRename" + count.getAndIncrement());
          }
        });
    try {
      ((ThreadPoolExecutor)executor).prestartAllCoreThreads();
      Future<Boolean>[] futures = new Future[concurrentRenames];
      for (int i = 0; i < concurrentRenames; i++) {
        final int index = i;
        futures[i] = executor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            NanoTimer timer = new NanoTimer();
            boolean result = fs.rename(source[index], target[index]);
            timer.end("parallel rename %d", index);
            LOG.info("Rename {} ran from {} to {}", index,
                timer.getStartTime(), timer.getEndTime());
            return result;
          }
        });
      }
      LOG.info("Waiting for tasks to complete...");
      LOG.info("Deadlock may have occurred if nothing else is logged" +
          " or the test times out");
      for (int i = 0; i < concurrentRenames; i++) {
        assertTrue("No future " + i, futures[i].get());
        assertPathExists("target path", target[i]);
        assertPathDoesNotExist("source path", source[i]);
      }
      LOG.info("All tasks have completed successfully");
    } finally {
      executor.shutdown();
    }
  }


  /**
   * Attempts to trigger a deadlock that would happen if any bounded resource
   * pool became saturated with control tasks that depended on other tasks
   * that now can't enter the resource pool to get completed.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testParallelRename() throws InterruptedException,
      ExecutionException, IOException {

    Configuration conf = getConfiguration();
    conf.setInt(MAX_THREADS, 2);
    conf.setInt(MAX_TOTAL_TASKS, 1);

    conf.set(MIN_MULTIPART_THRESHOLD, "10K");
    conf.set(MULTIPART_SIZE, "5K");

    try (S3AFileSystem tinyThreadPoolFs = new S3AFileSystem()) {
      tinyThreadPoolFs.initialize(auxFs.getUri(), conf);

      parallelRenames(concurrentRenames, tinyThreadPoolFs,
          "testParallelRename-source", "testParallelRename-target");
    }
  }

  @Test
  public void testThreadPoolCoolDown() throws InterruptedException,
      ExecutionException, IOException {

    int hotThreads = 0;
    int coldThreads = 0;

    parallelRenames(concurrentRenames, auxFs,
        "testThreadPoolCoolDown-source", "testThreadPoolCoolDown-target");

    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().startsWith("s3a-transfer")) {
        hotThreads++;
      }
    }

    int timeoutMs = Constants.DEFAULT_KEEPALIVE_TIME * 1000;
    Thread.sleep((int)(1.1 * timeoutMs));

    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().startsWith("s3a-transfer")) {
        coldThreads++;
      }
    }

    assertNotEquals("Failed to find threads in active FS - test is flawed",
        hotThreads, 0);
    assertTrue("s3a-transfer threads went from " + hotThreads + " to " +
        coldThreads + ", should have gone to 0", 0 == coldThreads);

  }
}
