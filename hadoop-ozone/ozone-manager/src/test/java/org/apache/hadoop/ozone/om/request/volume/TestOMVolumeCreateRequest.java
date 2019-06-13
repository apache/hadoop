/**
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

package org.apache.hadoop.ozone.om.request.volume;

import java.util.UUID;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeInfo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests create volume request.
 */
public class TestOMVolumeCreateRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;


  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();
    doPreExecute(volumeName, adminName, ownerName);
  }


  @Test
  public void testValidateAndUpdateCacheWithZeroMaxUserVolumeCount()
      throws Exception {
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(0L);
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    omVolumeCreateRequest.preExecute(ozoneManager);

    try {
      OMClientResponse omClientResponse =
          omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains("should be greater than zero",
          ex);
    }

  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    omVolumeCreateRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getVolumeTable().get(volumeKey));
    Assert.assertNull(omMetadataManager.getUserTable().get(ownerKey));

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    // Get volumeInfo from request.
    VolumeInfo volumeInfo = omVolumeCreateRequest.getOmRequest()
        .getCreateVolumeRequest().getVolumeInfo();

    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should not have entry.
    Assert.assertNotNull(omVolumeArgs);

    // Check data from table and request.
    Assert.assertEquals(volumeInfo.getVolume(), omVolumeArgs.getVolume());
    Assert.assertEquals(volumeInfo.getOwnerName(), omVolumeArgs.getOwnerName());
    Assert.assertEquals(volumeInfo.getAdminName(), omVolumeArgs.getAdminName());
    Assert.assertEquals(volumeInfo.getCreationTime(),
        omVolumeArgs.getCreationTime());

    OzoneManagerProtocolProtos.VolumeList volumeList = omMetadataManager
        .getUserTable().get(ownerKey);
    Assert.assertNotNull(volumeList);
    Assert.assertEquals(volumeName, volumeList.getVolumeNames(0));

  }


  @Test
  public void testValidateAndUpdateCacheWithVolumeAlreadyExists()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    omVolumeCreateRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_ALREADY_EXISTS,
        omResponse.getStatus());

    // Check really if we have a volume with the specified volume name.
    Assert.assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));

  }


  private void doPreExecute(String volumeName,
      String adminName, String ownerName) throws Exception {

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
  }

  /**
   * Verify modifiedOmRequest and originalRequest.
   * @param modifiedRequest
   * @param originalRequest
   */
  private void verifyRequest(OMRequest modifiedRequest,
      OMRequest originalRequest) {
    VolumeInfo original = originalRequest.getCreateVolumeRequest()
        .getVolumeInfo();
    VolumeInfo updated = modifiedRequest.getCreateVolumeRequest()
        .getVolumeInfo();

    Assert.assertEquals(original.getAdminName(), updated.getAdminName());
    Assert.assertEquals(original.getVolume(), updated.getVolume());
    Assert.assertEquals(original.getOwnerName(),
        updated.getOwnerName());
    Assert.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
  }

  /**
   * Create OMRequest for create volume.
   * @param volumeName
   * @param adminName
   * @param ownerName
   * @return OMRequest
   */
  private OMRequest createVolumeRequest(String volumeName, String adminName,
      String ownerName) {
    VolumeInfo volumeInfo = VolumeInfo.newBuilder().setVolume(volumeName)
        .setAdminName(adminName).setOwnerName(ownerName).build();
    CreateVolumeRequest createVolumeRequest =
        CreateVolumeRequest.newBuilder().setVolumeInfo(volumeInfo).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setCreateVolumeRequest(createVolumeRequest).build();
  }
}
