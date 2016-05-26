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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.ipc.AsyncCallLimitExceededException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAsyncDFSRename {
  public static final Log LOG = LogFactory.getLog(TestAsyncDFSRename.class);
  private final short replFactor = 1;
  private final long blockSize = 512;
  private long fileLen = blockSize * 3;
  private static final int NUM_TESTS = 50;
  private static final int NUM_NN_HANDLER = 10;
  private static final int ASYNC_CALL_LIMIT = 1000;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private AsyncDistributedFileSystem adfs;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    // set the limit of max async calls
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        ASYNC_CALL_LIMIT);
    // set server handlers
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, NUM_NN_HANDLER);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = FileSystem.get(conf);
    adfs = cluster.getFileSystem().getAsyncDistributedFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testCallGetReturnValueMultipleTimes() throws Exception {
    final Path parent = new Path("/test/testCallGetReturnValueMultipleTimes/");
    assertTrue(fs.mkdirs(parent));

    // prepare test
    final Path[] srcs = new Path[NUM_TESTS];
    final Path[] dsts = new Path[NUM_TESTS];
    for (int i = 0; i < NUM_TESTS; i++) {
      srcs[i] = new Path(parent, "src" + i);
      dsts[i] = new Path(parent, "dst" + i);
      DFSTestUtil.createFile(fs, srcs[i], fileLen, replFactor, 1);
      DFSTestUtil.createFile(fs, dsts[i], fileLen, replFactor, 1);
    }

    // concurrently invoking many rename
    final Map<Integer, Future<Void>> reFutures =
        new HashMap<Integer, Future<Void>>();
    for (int i = 0; i < NUM_TESTS; i++) {
      Future<Void> retFuture = adfs.rename(srcs[i], dsts[i],
          Rename.OVERWRITE);
      reFutures.put(i, retFuture);
    }

    assertEquals(NUM_TESTS, reFutures.size());

    for (int i = 0; i < 5; i++) {
      verifyCallGetReturnValueMultipleTimes(reFutures, srcs, dsts);
    }
  }

  private void verifyCallGetReturnValueMultipleTimes(
      final Map<Integer, Future<Void>> reFutures, final Path[] srcs,
      final Path[] dsts)
      throws InterruptedException, ExecutionException, IOException {

    // wait for completing the calls
    waitForReturnValues(reFutures, 0, NUM_TESTS);

    // verify the src dir should not exist, dst should
    verifyRenames(srcs, dsts);
  }

  @Test(timeout = 60000)
  public void testConcurrentAsyncRename() throws Exception {
    final Path parent = new Path(
        String.format("/test/%s/", "testConcurrentAsyncRename"));
    assertTrue(fs.mkdirs(parent));

    // prepare test
    final Path[] srcs = new Path[NUM_TESTS];
    final Path[] dsts = new Path[NUM_TESTS];
    for (int i = 0; i < NUM_TESTS; i++) {
      srcs[i] = new Path(parent, "src" + i);
      dsts[i] = new Path(parent, "dst" + i);
      DFSTestUtil.createFile(fs, srcs[i], fileLen, replFactor, 1);
      DFSTestUtil.createFile(fs, dsts[i], fileLen, replFactor, 1);
    }

    // concurrently invoking many rename
    int start = 0, end = 0;
    Map<Integer, Future<Void>> retFutures =
        new HashMap<Integer, Future<Void>>();
    for (int i = 0; i < NUM_TESTS; i++) {
      for (;;) {
        try {
          LOG.info("rename #" + i);
          Future<Void> retFuture = adfs.rename(srcs[i], dsts[i],
              Rename.OVERWRITE);
          retFutures.put(i, retFuture);
          break;
        } catch (AsyncCallLimitExceededException e) {
          /**
           * reached limit of async calls, fetch results of finished async calls
           * to let follow-on calls go
           */
          LOG.error(e);
          start = end;
          end = i;
          LOG.info(String.format("start=%d, end=%d, i=%d", start, end, i));
          waitForReturnValues(retFutures, start, end);
        }
      }
    }

    // wait for completing the calls
    waitForReturnValues(retFutures, end, NUM_TESTS);

    // verify the src dir should not exist, dst should
    verifyRenames(srcs, dsts);
  }

  private void verifyRenames(final Path[] srcs, final Path[] dsts)
      throws IOException {
    for (int i = 0; i < NUM_TESTS; i++) {
      assertFalse(fs.exists(srcs[i]));
      assertTrue(fs.exists(dsts[i]));
    }
  }

  void waitForReturnValues(final Map<Integer, Future<Void>> retFutures,
      final int start, final int end)
      throws InterruptedException, ExecutionException {
    TestAsyncDFS.waitForReturnValues(retFutures, start, end);
  }
}