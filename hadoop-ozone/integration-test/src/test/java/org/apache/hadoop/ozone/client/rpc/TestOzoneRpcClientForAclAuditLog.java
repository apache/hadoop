package org.apache.hadoop.ozone.client.rpc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 */
public class TestOzoneRpcClientForAclAuditLog extends
    TestOzoneRpcClientAbstract {

  private static UserGroupInformation ugi;
  private static final OzoneAcl USER_ACL =
      new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
      "johndoe", IAccessAuthorizer.ACLType.ALL, ACCESS);
  private static final OzoneAcl USER_ACL_2 =
      new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
      "jane", IAccessAuthorizer.ACLType.ALL, ACCESS);
  private static List<OzoneAcl> aclListToAdd = new ArrayList<>();

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    System.setProperty("log4j.configurationFile", "log4j2.properties");
    ugi = UserGroupInformation.getCurrentUser();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
    aclListToAdd.add(USER_ACL);
    aclListToAdd.add(USER_ACL_2);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    shutdownCluster();
    File file = new File("audit.log");
    if (FileUtils.deleteQuietly(file)) {
      LOG.info(file.getName() +
          " has been deleted as all tests have completed.");
    } else {
      LOG.info("audit.log could not be deleted.");
    }
  }

  @Test
  public void testXXXAclSuccessAudits() throws Exception {

    String userName = ugi.getUserName();
    String adminName = ugi.getUserName();
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    org.apache.hadoop.ozone.client.VolumeArgs createVolumeArgs =
        org.apache.hadoop.ozone.client.VolumeArgs.newBuilder()
            .setAdmin(adminName)
            .setOwner(userName)
            .build();
    getStore().createVolume(volumeName, createVolumeArgs);
    verifyLog(OMAction.CREATE_VOLUME.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    OzoneVolume retVolumeinfo = getStore().getVolume(volumeName);
    verifyLog(OMAction.READ_VOLUME.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    Assert.assertTrue(retVolumeinfo.getName().equalsIgnoreCase(volumeName));

    OzoneObj volObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();

    //Testing getAcl
    List<OzoneAcl> acls = getStore().getAcl(volObj);
    verifyLog(OMAction.GET_ACL.name(), volumeName,
        AuditEventStatus.SUCCESS.name());
    Assert.assertTrue(acls.size() > 0);

    //Testing addAcl
    getStore().addAcl(volObj, USER_ACL);
    verifyLog(OMAction.ADD_ACL.name(), volumeName, "johndoe",
        AuditEventStatus.SUCCESS.name());

    //Testing removeAcl
    getStore().removeAcl(volObj, USER_ACL);
    verifyLog(OMAction.REMOVE_ACL.name(), volumeName, "johndoe",
        AuditEventStatus.SUCCESS.name());

    //Testing setAcl
    getStore().setAcl(volObj, aclListToAdd);
    verifyLog(OMAction.SET_ACL.name(), volumeName, "johndoe", "jane",
        AuditEventStatus.SUCCESS.name());

  }

  @Test
  public void testXXXAclFailureAudits() throws Exception {

    String userName = "bilbo";
    String adminName = "bilbo";
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    org.apache.hadoop.ozone.client.VolumeArgs createVolumeArgs =
        org.apache.hadoop.ozone.client.VolumeArgs.newBuilder()
            .setAdmin(adminName)
            .setOwner(userName)
            .build();
    getStore().createVolume(volumeName, createVolumeArgs);
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
      List<OzoneAcl> acls = getStore().getAcl(volObj);
    } catch (Exception ex) {
      verifyLog(OMAction.GET_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      getStore().addAcl(volObj, USER_ACL);
    } catch (Exception ex) {
      verifyLog(OMAction.ADD_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      getStore().removeAcl(volObj, USER_ACL);
    } catch (Exception ex) {
      verifyLog(OMAction.REMOVE_ACL.name(), volumeName,
          AuditEventStatus.FAILURE.name());
    }

    try{
      getStore().setAcl(volObj, aclListToAdd);
    } catch (Exception ex) {
      verifyLog(OMAction.SET_ACL.name(), volumeName, "johndoe", "jane",
          AuditEventStatus.FAILURE.name());
    }

  }

  private void verifyLog(String... expected) throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    final int retry = 5;
    int i = 0;
    while (lines.isEmpty() && i < retry) {
      lines = FileUtils.readLines(file, (String)null);
      try {
        Thread.sleep(500 * (i + 1));
      } catch(InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      i++;
    }

    // When log entry is expected, the log file will contain one line and
    // that must be equal to the expected string
    assertTrue(lines.size() != 0);
    for(String exp: expected){
      assertTrue(lines.get(0).contains(exp));
    }
    //empty the file
    lines.clear();
    FileUtils.writeLines(file, lines, false);
  }


  //Overriding all tests from base class as empty methods to avoid it from
  // getting executed. This was done as those tests don't rely on ACL enabled
  // with Native Authorizer. The current test class uses ACLs with Native
  // Authorizer and will thus cause the base class tests to fail.

  @Override
  public void testOMClientProxyProvider(){}

  @Override
  public void testSetVolumeQuota(){}

  @Override
  public void testDeleteVolume(){}

  @Override
  public void testCreateVolumeWithMetadata(){}

  @Override
  public void testCreateBucketWithMetadata(){}

  @Override
  public void testCreateBucket(){}

  @Override
  public void testCreateS3Bucket(){}

  @Override
  public void testCreateSecureS3Bucket(){}

  @Override
  public void testListS3Buckets(){}

  @Override
  public void testListS3BucketsFail(){}

  @Override
  public void testDeleteS3Bucket(){}

  @Override
  public void testDeleteS3NonExistingBucket(){}

  @Override
  public void testCreateS3BucketMapping(){}

  @Override
  public void testCreateBucketWithVersioning(){}

  @Override
  public void testCreateBucketWithStorageType(){}

  @Override
  public void testCreateBucketWithAcls(){}

  @Override
  public void testCreateBucketWithAllArgument(){}

  @Override
  public void testInvalidBucketCreation(){}

  @Override
  public void testAddBucketAcl(){}

  @Override
  public void testRemoveBucketAcl(){}

  @Override
  public void testSetBucketVersioning(){}

  @Override
  public void testSetBucketStorageType(){}

  @Override
  public void testDeleteBucket(){}

  @Override
  public void testPutKey(){}

  @Override
  public void testValidateBlockLengthWithCommitKey(){}

  @Override
  public void testGetKeyAndFileWithNetworkTopology(){}

  @Override
  public void testPutKeyRatisOneNode(){}

  @Override
  public void testPutKeyRatisThreeNodes(){}

  @Override
  public void testPutKeyRatisThreeNodesParallel(){}

  @Override
  public void testReadKeyWithVerifyChecksumFlagEnable(){}

  @Override
  public void testReadKeyWithVerifyChecksumFlagDisable(){}

  @Override
  public void testGetKeyDetails(){}

  @Override
  public void testReadKeyWithCorruptedData(){}

  @Override
  public void testReadKeyWithCorruptedDataWithMutiNodes(){}

  @Override
  public void testDeleteKey(){}

  @Override
  public void testRenameKey(){}

  @Override
  public void testListVolume(){}

  @Override
  public void testListBucket(){}

  @Override
  public void testListBucketsOnEmptyVolume(){}

  @Override
  public void testListKey(){}

  @Override
  public void testListKeyOnEmptyBucket(){}

  @Override
  public void testInitiateMultipartUploadWithReplicationInformationSet(){}

  @Override
  public void testInitiateMultipartUploadWithDefaultReplication(){}

  @Override
  public void testUploadPartWithNoOverride(){}

  @Override
  public void testUploadPartOverrideWithStandAlone(){}

  @Override
  public void testUploadPartOverrideWithRatis(){}

  @Override
  public void testNoSuchUploadError(){}

  @Override
  public void testMultipartUpload(){}

  @Override
  public void testMultipartUploadOverride(){}

  @Override
  public void testMultipartUploadWithPartsLessThanMinSize(){}

  @Override
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent(){}

  @Override
  public void testMultipartUploadWithPartsMisMatchWithIncorrectPartName(){}

  @Override
  public void testMultipartUploadWithMissingParts(){}

  @Override
  public void testAbortUploadFail(){}

  @Override
  public void testAbortUploadSuccessWithOutAnyParts(){}

  @Override
  public void testAbortUploadSuccessWithParts(){}

  @Override
  public void testListMultipartUploadParts(){}

  @Override
  public void testListMultipartUploadPartsWithContinuation(){}

  @Override
  public void testListPartsInvalidPartMarker(){}

  @Override
  public void testListPartsInvalidMaxParts(){}

  @Override
  public void testListPartsWithPartMarkerGreaterThanPartCount(){}

  @Override
  public void testListPartsWithInvalidUploadID(){}

  @Override
  public void testNativeAclsForVolume(){}

  @Override
  public void testNativeAclsForBucket(){}

  @Override
  public void testNativeAclsForKey(){}

  @Override
  public void testNativeAclsForPrefix(){}

}
