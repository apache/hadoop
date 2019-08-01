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

package org.apache.hadoop.ozone.om.request.volume.acl;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmOzoneAclMap;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.volume.TestOMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.ACCESS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.DEFAULT;

/**
 * Tests volume setAcl request.
 */
public class TestOMVolumeSetAclRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        TestOMRequestUtils.createVolumeSetAclRequest(volumeName,
            Lists.newArrayList(acl));

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeSetAclRequest.preExecute(
        ozoneManager);
    Assert.assertNotEquals(modifiedRequest, originalRequest);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OzoneAcl userAccessAcl = OzoneAcl.parseAcl("user:bilbo:rw[ACCESS]");
    OzoneAcl groupDefaultAcl =
        OzoneAcl.parseAcl("group:admin:rwdlncxy[DEFAULT]");

    List<OzoneAcl> acls = Lists.newArrayList(userAccessAcl, groupDefaultAcl);

    OMRequest originalRequest =
        TestOMRequestUtils.createVolumeSetAclRequest(volumeName, acls);

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    omVolumeSetAclRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);

    // Get Acl before validateAndUpdateCache.
    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should have entry.
    Assert.assertNotNull(omVolumeArgs);
    OmOzoneAclMap aclMapBeforeSet = omVolumeArgs.getAclMap();

    OMClientResponse omClientResponse =
        omVolumeSetAclRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());

    OmOzoneAclMap aclMapAfterSet = omMetadataManager
        .getVolumeTable().get(volumeKey).getAclMap();
    Assert.assertNotEquals(aclMapBeforeSet, aclMapAfterSet);

    // Acl is added to aclMapAfterSet
    Assert.assertEquals(2, aclMapAfterSet.getAcl().size());
    Assert.assertTrue("Default Acl should be set.",
        aclMapAfterSet.getAclsByScope(ACCESS).contains(userAccessAcl));
    Assert.assertTrue("Default Acl should be set.",
        aclMapAfterSet.getAclsByScope(DEFAULT).contains(groupDefaultAcl));
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        TestOMRequestUtils.createVolumeSetAclRequest(volumeName,
            Lists.newArrayList(acl));

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    omVolumeSetAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetAclRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());
  }
}
