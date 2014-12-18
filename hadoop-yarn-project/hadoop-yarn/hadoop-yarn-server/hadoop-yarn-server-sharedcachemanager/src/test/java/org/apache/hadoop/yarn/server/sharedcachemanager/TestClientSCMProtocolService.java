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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Basic unit tests for the Client to SCM Protocol Service.
 */
public class TestClientSCMProtocolService {
  private static File testDir = null;

  @BeforeClass
  public static void setupTestDirs() throws IOException {
    testDir = new File("target",
        TestSharedCacheUploaderService.class.getCanonicalName());
    testDir.delete();
    testDir.mkdirs();
    testDir = testDir.getAbsoluteFile();
  }

  @AfterClass
  public static void cleanupTestDirs() throws IOException {
    if (testDir != null) {
      testDir.delete();
    }
  }


  private ClientProtocolService service;
  private ClientSCMProtocol clientSCMProxy;
  private SCMStore store;
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @Before
  public void startUp() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCM_STORE_CLASS,
        InMemorySCMStore.class.getName());
    conf.set(YarnConfiguration.SHARED_CACHE_ROOT, testDir.getPath());
    AppChecker appChecker = spy(new DummyAppChecker());
    store = new InMemorySCMStore(appChecker);
    store.init(conf);
    store.start();

    service = new ClientProtocolService(store);
    service.init(conf);
    service.start();

    YarnRPC rpc = YarnRPC.create(new Configuration());

    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);

    clientSCMProxy =
        (ClientSCMProtocol) rpc.getProxy(ClientSCMProtocol.class, scmAddress,
            conf);
  }

  @After
  public void cleanUp() {
    if (store != null) {
      store.stop();
      store = null;
    }

    if (service != null) {
      service.stop();
      service = null;
    }

    if (clientSCMProxy != null) {
      RPC.stopProxy(clientSCMProxy);
      clientSCMProxy = null;
    }
  }

  @Test
  public void testUse_MissingEntry() throws Exception {
    long misses = ClientSCMMetrics.getInstance().getCacheMisses();
    UseSharedCacheResourceRequest request =
        recordFactory.newRecordInstance(UseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(1, 1L));
    assertNull(clientSCMProxy.use(request).getPath());
    assertEquals("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
        .getInstance().getCacheMisses() - misses);
  }

  @Test
  public void testUse_ExistingEntry_NoAppIds() throws Exception {
    // Pre-populate the SCM with one cache entry
    store.addResource("key1", "foo.jar");

    long hits = ClientSCMMetrics.getInstance().getCacheHits();

    UseSharedCacheResourceRequest request =
        recordFactory.newRecordInstance(UseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(2, 2L));
    // Expecting default depth of 3 and under the shared cache root dir
    String expectedPath = testDir.getAbsolutePath() + "/k/e/y/key1/foo.jar";
    assertEquals(expectedPath, clientSCMProxy.use(request).getPath());
    assertEquals(1, store.getResourceReferences("key1").size());
    assertEquals("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
        .getInstance().getCacheHits() - hits);

  }

  @Test
  public void testUse_ExistingEntry_OneId() throws Exception {
    // Pre-populate the SCM with one cache entry
    store.addResource("key1", "foo.jar");
    store.addResourceReference("key1",
        new SharedCacheResourceReference(createAppId(1, 1L), "user"));
    assertEquals(1, store.getResourceReferences("key1").size());
    long hits = ClientSCMMetrics.getInstance().getCacheHits();

    // Add a new distinct appId
    UseSharedCacheResourceRequest request =
        recordFactory.newRecordInstance(UseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(2, 2L));

    // Expecting default depth of 3 under the shared cache root dir
    String expectedPath = testDir.getAbsolutePath() + "/k/e/y/key1/foo.jar";
    assertEquals(expectedPath, clientSCMProxy.use(request).getPath());
    assertEquals(2, store.getResourceReferences("key1").size());
    assertEquals("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
        .getInstance().getCacheHits() - hits);
  }

  @Test
  public void testUse_ExistingEntry_DupId() throws Exception {
    // Pre-populate the SCM with one cache entry
    store.addResource("key1", "foo.jar");
    UserGroupInformation testUGI = UserGroupInformation.getCurrentUser();
    store.addResourceReference("key1",
        new SharedCacheResourceReference(createAppId(1, 1L),
            testUGI.getShortUserName()));
    assertEquals(1, store.getResourceReferences("key1").size());

    long hits = ClientSCMMetrics.getInstance().getCacheHits();

    // Add a new duplicate appId
    UseSharedCacheResourceRequest request =
        recordFactory.newRecordInstance(UseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(1, 1L));

    // Expecting default depth of 3 under the shared cache root dir
    String expectedPath = testDir.getAbsolutePath() + "/k/e/y/key1/foo.jar";
    assertEquals(expectedPath, clientSCMProxy.use(request).getPath());
    assertEquals(1, store.getResourceReferences("key1").size());

    assertEquals("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
        .getInstance().getCacheHits() - hits);
  }

  @Test
  public void testRelease_ExistingEntry_NonExistantAppId() throws Exception {
    // Pre-populate the SCM with one cache entry
    store.addResource("key1", "foo.jar");
    store.addResourceReference("key1",
        new SharedCacheResourceReference(createAppId(1, 1L), "user"));
    assertEquals(1, store.getResourceReferences("key1").size());

    long releases = ClientSCMMetrics.getInstance().getCacheReleases();

    ReleaseSharedCacheResourceRequest request =
        recordFactory
            .newRecordInstance(ReleaseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(2, 2L));
    clientSCMProxy.release(request);
    assertEquals(1, store.getResourceReferences("key1").size());

    assertEquals(
        "Client SCM metrics were updated when a release did not happen", 0,
        ClientSCMMetrics.getInstance().getCacheReleases() - releases);

  }

  @Test
  public void testRelease_ExistingEntry_WithAppId() throws Exception {
    // Pre-populate the SCM with one cache entry
    store.addResource("key1", "foo.jar");
    UserGroupInformation testUGI = UserGroupInformation.getCurrentUser();
    store.addResourceReference("key1",
        new SharedCacheResourceReference(createAppId(1, 1L),
            testUGI.getShortUserName()));
    assertEquals(1, store.getResourceReferences("key1").size());

    long releases = ClientSCMMetrics.getInstance().getCacheReleases();

    ReleaseSharedCacheResourceRequest request =
        recordFactory
            .newRecordInstance(ReleaseSharedCacheResourceRequest.class);
    request.setResourceKey("key1");
    request.setAppId(createAppId(1, 1L));
    clientSCMProxy.release(request);
    assertEquals(0, store.getResourceReferences("key1").size());

    assertEquals("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
        .getInstance().getCacheReleases() - releases);

  }

  @Test
  public void testRelease_MissingEntry() throws Exception {

    long releases = ClientSCMMetrics.getInstance().getCacheReleases();

    ReleaseSharedCacheResourceRequest request =
        recordFactory
            .newRecordInstance(ReleaseSharedCacheResourceRequest.class);
    request.setResourceKey("key2");
    request.setAppId(createAppId(2, 2L));
    clientSCMProxy.release(request);
    assertNotNull(store.getResourceReferences("key2"));
    assertEquals(0, store.getResourceReferences("key2").size());
    assertEquals(
        "Client SCM metrics were updated when a release did not happen.", 0,
        ClientSCMMetrics.getInstance().getCacheReleases() - releases);
  }

  private ApplicationId createAppId(int id, long timestamp) {
    return ApplicationId.newInstance(timestamp, id);
  }
}
