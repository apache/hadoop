/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.Assert.assertTrue;

/**
 * This class is to test audit logs for xxxACL APIs of Ozone Client.
 * It is annotated as NotThreadSafe intentionally since this test reads from
 * the generated audit logs to verify the operations. Since the
 * maven test plugin will trigger parallel test execution, there is a
 * possibility of other audit events being logged and leading to failure of
 * all assertion based test in this class.
 */
@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore("Fix this after adding audit support for HA Acl code. This will be " +
    "fixed by HDDS-2038")
public class TestOzoneRpcClientForAclAuditLog {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneRpcClientForAclAuditLog.class);
  private static UserGroupInformation ugi;
  private static final OzoneAcl USER_ACL =
      new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
      "johndoe", IAccessAuthorizer.ACLType.ALL, ACCESS);
  private static final OzoneAcl USER_ACL_2 =
      new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
      "jane", IAccessAuthorizer.ACLType.ALL, ACCESS);
  private static List<OzoneAcl> aclListToAdd = new ArrayList<>();
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String scmId = UUID.randomUUID().toString();


  /**
   * Create a MiniOzoneCluster for testing.
   *
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
    ugi = UserGroupInformation.getCurrentUser();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
    aclListToAdd.add(USER_ACL);
    aclListToAdd.add(USER_ACL_2);
    emptyAuditLog();
  }

  /**
   * Create a MiniOzoneCluster for testing.
   * @param conf Configurations to start the cluster.
   * @throws Exception
   */
  private static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setScmId(scmId)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void teardown() throws IOException {
    shutdownCluster();
    deleteAuditLog();
  }

  private static void deleteAuditLog() throws IOException {
    File file = new File("audit.log");
    if (FileUtils.deleteQuietly(file)) {
      LOG.info(file.getName() +
          " has been deleted.");
    } else {
      LOG.info("audit.log could not be deleted.");
    }
  }

  private static void emptyAuditLog() throws IOException {
    File file = new File("audit.log");
    FileUtils.writeLines(file, new ArrayList<>(), false);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  private static void shutdownCluster() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testXXXAclSuccessAudits() throws Exception {

    String userName = ugi.getUserName();
    String adminName = ugi.getUserName();
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();
    store.createVolume(volumeName, createVolumeArgs);
    verifyLog(OMAction.CREATE_VOLUME.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    OzoneVolume retVolumeinfo = store.getVolume(volumeName);
    verifyLog(OMAction.READ_VOLUME.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    Assert.assertTrue(retVolumeinfo.getName().equalsIgnoreCase(volumeName));

    OzoneObj volObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();

    //Testing getAcl
    List<OzoneAcl> acls = store.getAcl(volObj);
    verifyLog(OMAction.GET_ACL.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    Assert.assertTrue(acls.size() > 0);

    //Testing addAcl
    store.addAcl(volObj, USER_ACL);
    verifyLog(OMAction.ADD_ACL.name(), volumeName, "johndoe",
        AuditEventStatus.SUCCESS.name());

    //Testing removeAcl
    store.removeAcl(volObj, USER_ACL);
    verifyLog(OMAction.REMOVE_ACL.name(), volumeName, "johndoe",
        AuditEventStatus.SUCCESS.name());

    //Testing setAcl
    store.setAcl(volObj, aclListToAdd);
    verifyLog(OMAction.SET_ACL.name(), volumeName, "johndoe", "jane",
        AuditEventStatus.SUCCESS.name());

  }

  @Test
  public void testXXXAclFailureAudits() throws Exception {

    String userName = "bilbo";
    String adminName = "bilbo";
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();
    store.createVolume(volumeName, createVolumeArgs);
    verifyLog(OMAction.CREATE_VOLUME.name(), volumeName,
        AuditEventStatus.SUCCESS.name());

    OzoneObj volObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();

    // xxxAcl will fail as current ugi user doesn't have the required access
    // for volume
    try{
      List<OzoneAcl> acls = store.getAcl(volObj);
    } catch (Exception ex) {
      verifyLog(OMAction.GET_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      store.addAcl(volObj, USER_ACL);
    } catch (Exception ex) {
      verifyLog(OMAction.ADD_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      store.removeAcl(volObj, USER_ACL);
    } catch (Exception ex) {
      verifyLog(OMAction.REMOVE_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      store.setAcl(volObj, aclListToAdd);
    } catch (Exception ex) {
      verifyLog(OMAction.SET_ACL.name(), volumeName, "johndoe", "jane",
          AuditEventStatus.FAILURE.name());
    }

  }

  private void verifyLog(String... expected) throws Exception {
    File file = new File("audit.log");
    final List<String> lines = FileUtils.readLines(file, (String)null);
    GenericTestUtils.waitFor(() ->
        (lines != null) ? true : false, 100, 60000);

    try{
      // When log entry is expected, the log file will contain one line and
      // that must be equal to the expected string
      assertTrue(lines.size() != 0);
      for(String exp: expected){
        assertTrue(lines.get(0).contains(exp));
      }
    } catch (AssertionError ex){
      LOG.error("Error occurred in log verification", ex);
      if(lines.size() != 0){
        LOG.error("Actual line ::: " + lines.get(0));
        LOG.error("Expected tokens ::: " + Arrays.toString(expected));
      }
      throw ex;
    } finally {
      emptyAuditLog();
    }
  }

}
