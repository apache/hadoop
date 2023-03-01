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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;


/**
 * Basic unit tests for the NodeManger to SCM Protocol Service.
 */
public class TestSharedCacheUploaderService {
  private static File testDir = null;

  @BeforeAll
  public static void setupTestDirs() throws IOException {
    testDir = new File("target",
        TestSharedCacheUploaderService.class.getCanonicalName());
    testDir.delete();
    testDir.mkdirs();
    testDir = testDir.getAbsoluteFile();
  }

  @AfterAll
  public static void cleanupTestDirs() throws IOException {
    if (testDir != null) {
      testDir.delete();
    }
  }

  private SharedCacheUploaderService service;
  private SCMUploaderProtocol proxy;
  private SCMStore store;
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @BeforeEach
  public void startUp() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCM_STORE_CLASS,
        InMemorySCMStore.class.getName());
    conf.set(YarnConfiguration.SHARED_CACHE_ROOT, testDir.getPath());
    AppChecker appChecker = spy(new DummyAppChecker());
    store = new InMemorySCMStore(appChecker);
    store.init(conf);
    store.start();

    service = new SharedCacheUploaderService(store);
    service.init(conf);
    service.start();

    YarnRPC rpc = YarnRPC.create(new Configuration());

    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_PORT);

    proxy =
        (SCMUploaderProtocol) rpc.getProxy(
            SCMUploaderProtocol.class, scmAddress, conf);
  }

  @AfterEach
  public void cleanUp() {
    if (store != null) {
      store.stop();
    }

    if (service != null) {
      service.stop();
    }

    if (proxy != null) {
      RPC.stopProxy(proxy);
    }
  }

  @Test
  void testNotify_noEntry() throws Exception {
    long accepted =
        SharedCacheUploaderMetrics.getInstance().getAcceptedUploads();

    SCMUploaderNotifyRequest request =
        recordFactory.newRecordInstance(SCMUploaderNotifyRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foo.jar");
    assertTrue(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());

    assertEquals(
        1,
        SharedCacheUploaderMetrics.getInstance().getAcceptedUploads() -
            accepted,
        "NM upload metrics aren't updated.");

  }

  @Test
  void testNotify_entryExists_differentName() throws Exception {

    long rejected =
        SharedCacheUploaderMetrics.getInstance().getRejectUploads();

    store.addResource("key1", "foo.jar");
    SCMUploaderNotifyRequest request =
        recordFactory.newRecordInstance(SCMUploaderNotifyRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foobar.jar");
    assertFalse(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());
    assertEquals(
        1,
        SharedCacheUploaderMetrics.getInstance().getRejectUploads() -
            rejected,
        "NM upload metrics aren't updated.");

  }

  @Test
  void testNotify_entryExists_sameName() throws Exception {

    long accepted =
        SharedCacheUploaderMetrics.getInstance().getAcceptedUploads();

    store.addResource("key1", "foo.jar");
    SCMUploaderNotifyRequest request =
        recordFactory.newRecordInstance(SCMUploaderNotifyRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foo.jar");
    assertTrue(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());
    assertEquals(
        1,
        SharedCacheUploaderMetrics.getInstance().getAcceptedUploads() -
            accepted,
        "NM upload metrics aren't updated.");

  }
}
