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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.util.LightWeightCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRetryCacheWithHA {
  private static final Log LOG = LogFactory.getLog(TestRetryCacheWithHA.class);
  
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;
  private static Configuration conf = new HdfsConfiguration();
  
  private static final int BlockSize = 1024;
  private static final short DataNodes = 3;
  private final static Map<String, Object> results = 
      new HashMap<String, Object>();
  
  /** 
   * A dummy invocation handler extending RetryInvocationHandler. We can use
   * a boolean flag to control whether the method invocation succeeds or not. 
   */
  private static class DummyRetryInvocationHandler extends
      RetryInvocationHandler<ClientProtocol> {
    static AtomicBoolean block = new AtomicBoolean(false);

    DummyRetryInvocationHandler(
        FailoverProxyProvider<ClientProtocol> proxyProvider,
        RetryPolicy retryPolicy) {
      super(proxyProvider, retryPolicy);
    }

    @Override
    protected Object invokeMethod(Method method, Object[] args)
        throws Throwable {
      Object result = super.invokeMethod(method, args);
      if (block.get()) {
        throw new UnknownHostException("Fake Exception");
      } else {
        return result;
      }
    }
  }
  
  @Before
  public void setup() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(DataNodes).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
    // setup the configuration
    HATestUtil.setFailoverConfigurations(cluster, conf);
    dfs = (DistributedFileSystem) HATestUtil.configureFailoverFs(cluster, conf);
  }
  
  @After
  public void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * 1. Run a set of operations
   * 2. Trigger the NN failover
   * 3. Check the retry cache on the original standby NN
   */
  @Test
  public void testRetryCacheOnStandbyNN() throws Exception {
    // 1. run operations
    DFSTestUtil.runOperations(cluster, dfs, conf, BlockSize, 0);
    
    // check retry cache in NN1
    FSNamesystem fsn0 = cluster.getNamesystem(0);
    LightWeightCache<CacheEntry, CacheEntry> cacheSet = 
        (LightWeightCache<CacheEntry, CacheEntry>) fsn0.getRetryCache().getCacheSet();
    assertEquals(14, cacheSet.size());
    
    Map<CacheEntry, CacheEntry> oldEntries = 
        new HashMap<CacheEntry, CacheEntry>();
    Iterator<CacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // 2. Failover the current standby to active.
    cluster.getNameNode(0).getRpcServer().rollEditLog();
    cluster.getNameNode(1).getNamesystem().getEditLogTailer().doTailEdits();
    
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    
    // 3. check the retry cache on the new active NN
    FSNamesystem fsn1 = cluster.getNamesystem(1);
    cacheSet = (LightWeightCache<CacheEntry, CacheEntry>) fsn1
        .getRetryCache().getCacheSet();
    assertEquals(14, cacheSet.size());
    iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      assertTrue(oldEntries.containsKey(entry));
    }
  }
  
  private DFSClient genClientWithDummyHandler() throws IOException {
    URI nnUri = dfs.getUri();
    Class<FailoverProxyProvider<ClientProtocol>> failoverProxyProviderClass = 
        NameNodeProxies.getFailoverProxyProviderClass(conf, nnUri, 
            ClientProtocol.class);
    FailoverProxyProvider<ClientProtocol> failoverProxyProvider = 
        NameNodeProxies.createFailoverProxyProvider(conf, 
            failoverProxyProviderClass, ClientProtocol.class, nnUri);
    InvocationHandler dummyHandler = new DummyRetryInvocationHandler(
        failoverProxyProvider, RetryPolicies
        .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            Integer.MAX_VALUE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT));
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        failoverProxyProvider.getInterface().getClassLoader(),
        new Class[] { ClientProtocol.class }, dummyHandler);
    
    DFSClient client = new DFSClient(null, proxy, conf, null);
    return client;
  }
  
  /**
   * When NN failover happens, if the client did not receive the response and
   * send a retry request to the other NN, the same response should be recieved
   * based on the retry cache.
   * 
   * TODO: currently we only test the createSnapshot from the client side. We 
   * may need to cover all the calls with "@AtMostOnce" annotation.
   */
  @Test
  public void testClientRetryWithFailover() throws Exception {
    final String dir = "/test";
    final Path dirPath = new Path(dir);
    final String sName = "s1";
    final String dirSnapshot = dir + HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR
        + Path.SEPARATOR + sName;
    
    dfs.mkdirs(dirPath);
    dfs.allowSnapshot(dirPath);
    
    final DFSClient client = genClientWithDummyHandler();
    // set DummyRetryInvocationHandler#block to true
    DummyRetryInvocationHandler.block.set(true);
    
    new Thread() {
      @Override
      public void run() {
        try {
          final String snapshotPath = client.createSnapshot(dir, "s1");
          assertEquals(dirSnapshot, snapshotPath);
          LOG.info("Created snapshot " + snapshotPath);
          synchronized (TestRetryCacheWithHA.this) {
            results.put("createSnapshot", snapshotPath);
            TestRetryCacheWithHA.this.notifyAll();
          }
        } catch (IOException e) {
          LOG.info("Got IOException " + e + " while creating snapshot");
        } finally {
          IOUtils.cleanup(null, client);
        }
      }
    }.start();
    
    // make sure the client's createSnapshot call has actually been handled by
    // the active NN
    boolean snapshotCreated = dfs.exists(new Path(dirSnapshot));
    while (!snapshotCreated) {
      Thread.sleep(1000);
      snapshotCreated = dfs.exists(new Path(dirSnapshot));
    }
    
    // force the failover
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    // disable the block in DummyHandler
    LOG.info("Setting block to false");
    DummyRetryInvocationHandler.block.set(false);
    
    synchronized (this) {
      while (!results.containsKey("createSnapshot")) {
        this.wait();
      }
      LOG.info("Got the result of createSnapshot: "
          + results.get("createSnapshot"));
    }
  }
}