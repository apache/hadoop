/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for Ozone Manager ACLs.
 */
public class TestOmAcls {

  private static boolean aclAllow = true;
  private static MiniOzoneCluster cluster = null;
  private static OMMetrics omMetrics;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static GenericTestUtils.LogCapturer logCapturer;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizerTest.class,
        IAccessAuthorizer.class);
    conf.setStrings(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    omMetrics = cluster.getOzoneManager().getMetrics();
    logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.getLogger());
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests the OM Initialization.
   */

  @Test
  public void testBucketCreationPermissionDenied() throws Exception {

    TestOmAcls.aclAllow = true;

    String volumeName = RandomStringUtils.randomAlphabetic(5).toLowerCase();
    String bucketName = RandomStringUtils.randomAlphabetic(5).toLowerCase();
    cluster.getClient().getObjectStore().createVolume(volumeName);
    OzoneVolume volume =
        cluster.getClient().getObjectStore().getVolume(volumeName);

    TestOmAcls.aclAllow = false;
    OzoneTestUtils.expectOmException(ResultCodes.PERMISSION_DENIED,
        () -> volume.createBucket(bucketName));

    assertTrue(logCapturer.getOutput()
        .contains("doesn't have CREATE permission to access bucket"));
  }

  @Test
  public void testFailureInKeyOp() throws Exception {
    final VolumeArgs createVolumeArgs;

    TestOmAcls.aclAllow = true;
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    logCapturer.clearOutput();

    TestOmAcls.aclAllow = false;

    OzoneTestUtils.expectOmException(ResultCodes.PERMISSION_DENIED,
        () -> TestDataUtil.createKey(bucket, "testKey", "testcontent"));
    assertTrue(logCapturer.getOutput().contains("doesn't have CREATE " +
        "permission to access key"));
  }

  /**
   * Test implementation to negative case.
   */
  static class OzoneAccessAuthorizerTest implements IAccessAuthorizer {

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return TestOmAcls.aclAllow;
    }
  }

}
