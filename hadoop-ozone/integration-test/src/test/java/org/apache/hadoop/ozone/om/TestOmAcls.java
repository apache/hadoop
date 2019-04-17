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

import java.util.LinkedList;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneAclException;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
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

  private static MiniOzoneCluster cluster = null;
  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
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
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthrizerTest.class,
        IAccessAuthorizer.class);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
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
  public void testOMAclsPermissionDenied() throws Exception {
    String user0 = "testListVolumes-user-0";
    String adminUser = "testListVolumes-admin";
    final VolumeArgs createVolumeArgs;
    int i = 100;
    String user0VolName = "Vol-" + user0 + "-" + i;
    createVolumeArgs = new VolumeArgs(user0VolName, userArgs);
    createVolumeArgs.setUserName(user0);
    createVolumeArgs.setAdminName(adminUser);
    createVolumeArgs.setQuota(new OzoneQuota(i, OzoneQuota.Units.GB));
    logCapturer.clearOutput();
    OzoneTestUtils.expectOmException(ResultCodes.INTERNAL_ERROR,
        () -> storageHandler.createVolume(createVolumeArgs));
    assertTrue(logCapturer.getOutput().contains("doesn't have CREATE " +
        "permission to access volume"));

    BucketArgs bucketArgs = new BucketArgs("bucket1", createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    OzoneTestUtils.expectOmException(ResultCodes.INTERNAL_ERROR,
        () -> storageHandler.createBucket(bucketArgs));
    assertTrue(logCapturer.getOutput().contains("doesn't have CREATE " +
        "permission to access bucket"));
  }

  @Test
  public void testFailureInKeyOp() throws Exception {
    final VolumeArgs createVolumeArgs;
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    createVolumeArgs = new VolumeArgs(userName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    createVolumeArgs.setQuota(new OzoneQuota(100, OzoneQuota.Units.GB));
    BucketArgs bucketArgs = new BucketArgs("bucket1", createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    logCapturer.clearOutput();

    // write a key without specifying size at all
    String keyName = "testKey";
    KeyArgs keyArgs = new KeyArgs(keyName, bucketArgs);
    OzoneTestUtils.expectOmException(ResultCodes.INTERNAL_ERROR,
        () -> storageHandler.newKeyWriter(keyArgs));
    assertTrue(logCapturer.getOutput().contains("doesn't have READ permission" +
        " to access key"));
  }
}

/**
 * Test implementation to negative case.
 */
class OzoneAccessAuthrizerTest implements IAccessAuthorizer {

  @Override
  public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OzoneAclException {
    return false;
  }
}
