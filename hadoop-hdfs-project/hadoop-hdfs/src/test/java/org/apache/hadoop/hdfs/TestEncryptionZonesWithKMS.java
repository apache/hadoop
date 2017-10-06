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

import static org.junit.Assert.assertTrue;

import com.google.common.base.Supplier;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;

public class TestEncryptionZonesWithKMS extends TestEncryptionZones {

  private MiniKMS miniKMS;

  @Override
  protected String getKeyProviderURI() {
    return KMSClientProvider.SCHEME_NAME + "://" +
        miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  @Before
  public void setup() throws Exception {
    File kmsDir = new File("target/test-classes/" +
        UUID.randomUUID().toString());
    Assert.assertTrue(kmsDir.mkdirs());
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
    miniKMS.start();
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
    miniKMS.stop();
  }
  
  @Override
  protected void setProvider() {
  }

  private KMSClientProvider getKMSClientProvider() {
    LoadBalancingKMSClientProvider lbkmscp =
        (LoadBalancingKMSClientProvider) Whitebox
        .getInternalState(cluster.getNamesystem().getProvider(), "extension");
    assert lbkmscp.getProviders().length == 1;
    return lbkmscp.getProviders()[0];
  }

  @Test(timeout = 120000)
  public void testCreateEZPopulatesEDEKCache() throws Exception {
    final Path zonePath = new Path("/TestEncryptionZone");
    fsWrapper.mkdir(zonePath, FsPermission.getDirDefault(), false);
    dfsAdmin.createEncryptionZone(zonePath, TEST_KEY, NO_TRASH);
    @SuppressWarnings("unchecked")
    KMSClientProvider kcp = getKMSClientProvider();
    assertTrue(kcp.getEncKeyQueueSize(TEST_KEY) > 0);
  }

  @Test(timeout = 120000)
  public void testDelegationToken() throws Exception {
    final String renewer = "JobTracker";
    UserGroupInformation.createRemoteUser(renewer);

    Credentials creds = new Credentials();
    Token<?> tokens[] = fs.addDelegationTokens(renewer, creds);
    DistributedFileSystem.LOG.debug("Delegation tokens: " +
        Arrays.asList(tokens));
    Assert.assertEquals(2, tokens.length);
    Assert.assertEquals(2, creds.numberOfTokens());
    
    // If the dt exists, will not get again
    tokens = fs.addDelegationTokens(renewer, creds);
    Assert.assertEquals(0, tokens.length);
    Assert.assertEquals(2, creds.numberOfTokens());
  }

  @Test(timeout = 120000)
  public void testWarmupEDEKCacheOnStartup() throws Exception {
    Path zonePath = new Path("/TestEncryptionZone");
    fsWrapper.mkdir(zonePath, FsPermission.getDirDefault(), false);
    dfsAdmin.createEncryptionZone(zonePath, TEST_KEY, NO_TRASH);
    final String anotherKey = "k2";
    zonePath = new Path("/TestEncryptionZone2");
    DFSTestUtil.createKey(anotherKey, cluster, conf);
    fsWrapper.mkdir(zonePath, FsPermission.getDirDefault(), false);
    dfsAdmin.createEncryptionZone(zonePath, anotherKey, NO_TRASH);

    @SuppressWarnings("unchecked")
    KMSClientProvider spy = getKMSClientProvider();
    assertTrue("key queue is empty after creating encryption zone",
        spy.getEncKeyQueueSize(TEST_KEY) > 0);

    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, 0);
    cluster.restartNameNode(true);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        final KMSClientProvider kspy = getKMSClientProvider();
        return kspy.getEncKeyQueueSize(TEST_KEY) > 0;
      }
    }, 1000, 60000);
  }
}
