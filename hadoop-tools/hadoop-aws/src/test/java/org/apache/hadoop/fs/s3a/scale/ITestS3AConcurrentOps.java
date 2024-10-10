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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils.NanoTimer;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

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
  protected Configuration createScaleConfiguration() {
    final Configuration conf = super.createScaleConfiguration();
    removeBaseAndBucketOverrides(conf, MULTIPART_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();
    assume("multipart upload/copy disabled",
        conf.getBoolean(MULTIPART_UPLOADS_ENABLED, true));
    auxFs = getNormalFileSystem();

    // this is set to the method path, even in test setup.
    testRoot = methodPath();
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
      IntStream.range(0, concurrentRenames).forEachOrdered(i -> {
        futures[i] = executor.submit(() -> {
          NanoTimer timer = new NanoTimer();
          boolean result = fs.rename(source[i], target[i]);
          timer.end("parallel rename %d", i);
          LOG.info("Rename {} ran from {} to {}", i,
              timer.getStartTime(), timer.getEndTime());
          return result;
        });
      });
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
  public void testParallelRename() throws InterruptedException,
      ExecutionException, IOException {

    // clone the fs with all its per-bucket settings
    Configuration conf = new Configuration(getFileSystem().getConf());

    // shrink the thread pool
    conf.setInt(MAX_THREADS, 2);
    conf.setInt(MAX_TOTAL_TASKS, 1);

    try (S3AFileSystem tinyThreadPoolFs = new S3AFileSystem()) {
      tinyThreadPoolFs.initialize(auxFs.getUri(), conf);

      parallelRenames(concurrentRenames, tinyThreadPoolFs,
          "testParallelRename-source", "testParallelRename-target");
    }
  }

  /**
   * Verify that after a parallel rename batch there are multiple
   * transfer threads active -and that after the timeout duration
   * that thread count has dropped to zero.
   */
  @Test
  public void testThreadPoolCoolDown() throws InterruptedException,
      ExecutionException, IOException {


    parallelRenames(concurrentRenames, auxFs,
        "testThreadPoolCoolDown-source", "testThreadPoolCoolDown-target");

    int hotThreads = (int) Thread.getAllStackTraces()
        .keySet()
        .stream()
        .filter(t -> t.getName().startsWith("s3a-transfer"))
        .count();

    Assertions.assertThat(hotThreads)
        .describedAs("Failed to find threads in active FS - test is flawed")
        .isNotEqualTo(0);

    long timeoutMs = DEFAULT_KEEPALIVE_TIME_DURATION.toMillis();
    Thread.sleep((int)(1.1 * timeoutMs));

    int coldThreads = (int) Thread.getAllStackTraces()
        .keySet()
        .stream()
        .filter(t -> t.getName().startsWith("s3a-transfer"))
        .count();

    Assertions.assertThat(coldThreads)
        .describedAs(("s3a-transfer threads went from %s to %s;"
            + " should have gone to 0"),
            hotThreads, coldThreads)
        .isEqualTo(0);
  }
}
