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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.io.retry.AsyncCallHandler;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.AsyncGetFuture;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Test async methods with HA setup. */
public class TestAsyncHDFSWithHA {
  static final Logger LOG = LoggerFactory.getLogger(TestAsyncHDFSWithHA.class);
  static {
    GenericTestUtils.setLogLevel(RetryInvocationHandler.LOG, Level.ALL);
  }

  private static <T> Future<T> getReturnValue() {
    return new AsyncGetFuture<>(AsyncCallHandler.getAsyncReturn());
  }

  static void mkdirs(DistributedFileSystem dfs, String dir, Path[] srcs,
                     Path[] dsts) throws IOException {
    for (int i = 0; i < srcs.length; i++) {
      srcs[i] = new Path(dir, "src" + i);
      dsts[i] = new Path(dir, "dst" + i);
      dfs.mkdirs(srcs[i]);
    }
  }

  static void runTestAsyncWithoutRetry(Configuration conf,
      MiniDFSCluster cluster, DistributedFileSystem dfs) throws Exception {
    final int num = 5;

    final String renameDir = "/testAsyncWithoutRetry/";
    final Path[] srcs = new Path[num + 1];
    final Path[] dsts = new Path[num + 1];
    mkdirs(dfs, renameDir, srcs, dsts);

    // create a proxy without retry.
    final NameNodeProxiesClient.ProxyAndInfo<ClientProtocol> proxyInfo
        = NameNodeProxies.createNonHAProxy(conf,
        cluster.getNameNode(0).getNameNodeAddress(),
        ClientProtocol.class, UserGroupInformation.getCurrentUser(),
        false);
    final ClientProtocol cp = proxyInfo.getProxy();

    // submit async calls
    Client.setAsynchronousMode(true);
    final List<Future<Void>> results = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      final String src = srcs[i].toString();
      final String dst = dsts[i].toString();
      LOG.info(i + ") rename " + src + " -> " + dst);
      cp.rename2(src, dst);
      results.add(getReturnValue());
    }
    Client.setAsynchronousMode(false);

    // wait for the async calls
    for (Future<Void> f : results) {
      f.get();
    }

    //check results
    for (int i = 0; i < num; i++) {
      Assert.assertEquals(false, dfs.exists(srcs[i]));
      Assert.assertEquals(true, dfs.exists(dsts[i]));
    }
  }

  /** Testing HDFS async methods with HA setup. */
  @Test(timeout = 120000)
  public void testAsyncWithHAFailover() throws Exception {
    final int num = 10;

    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0).build();

    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      final DistributedFileSystem dfs = HATestUtil.configureFailoverFs(
          cluster, conf);
      runTestAsyncWithoutRetry(conf, cluster, dfs);

      final String renameDir = "/testAsyncWithHAFailover/";
      final Path[] srcs = new Path[num + 1];
      final Path[] dsts = new Path[num + 1];
      mkdirs(dfs, renameDir, srcs, dsts);

      // submit async calls and trigger failover in the middle.
      final AsyncDistributedFileSystem adfs
          = dfs.getAsyncDistributedFileSystem();
      final ExecutorService executor = Executors.newFixedThreadPool(num + 1);

      final List<Future<Void>> results = new ArrayList<>();
      final List<IOException> exceptions = new ArrayList<>();
      final List<Future<?>> futures = new ArrayList<>();
      final int half = num/2;
      for(int i = 0; i <= num; i++) {
        final int id = i;
        futures.add(executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              if (id == half) {
                // failover
                cluster.shutdownNameNode(0);
                cluster.transitionToActive(1);
              } else {
                // rename
                results.add(adfs.rename(srcs[id], dsts[id]));
              }
            } catch (IOException e) {
              exceptions.add(e);
            }
          }
        }));
      }

      // wait for the tasks
      Assert.assertEquals(num + 1, futures.size());
      for(int i = 0; i <= num; i++) {
        futures.get(i).get();
      }
      // wait for the async calls
      Assert.assertEquals(num, results.size());
      Assert.assertTrue(exceptions.isEmpty());
      for(Future<Void> r : results) {
        r.get();
      }

      // check results
      for(int i = 0; i <= num; i++) {
        final boolean renamed = i != half;
        Assert.assertEquals(!renamed, dfs.exists(srcs[i]));
        Assert.assertEquals(renamed, dfs.exists(dsts[i]));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}