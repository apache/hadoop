/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request;

import java.net.InetAddress;
import java.util.UUID;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.security.UserGroupInformation;

import static org.mockito.Mockito.when;

/**
 * Test OMClient Request with user information.
 */
public class TestOMClientRequestWithUserInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private UserGroupInformation userGroupInformation =
      UserGroupInformation.createRemoteUser("temp");
  private InetAddress inetAddress;

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
    inetAddress = InetAddress.getByName("127.0.0.1");

    new MockUp<ProtobufRpcEngine.Server>() {
      @Mock
      public UserGroupInformation getRemoteUser() {
        return userGroupInformation;
      }

      public InetAddress getRemoteAddress() {
        return inetAddress;
      }
    };
  }

  @Test
  public void testUserInfo() throws Exception {

    String bucketName = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        TestOMRequestUtils.createBucketRequest(bucketName, volumeName, true,
            OzoneManagerProtocolProtos.StorageTypeProto.DISK);

    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(omRequest);

    Assert.assertFalse(omRequest.hasUserInfo());

    OzoneManagerProtocolProtos.OMRequest modifiedRequest =
        omBucketCreateRequest.preExecute(ozoneManager);

    Assert.assertTrue(modifiedRequest.hasUserInfo());

    // Now pass modified request to OMBucketCreateRequest and check ugi and
    // remote Address.
    omBucketCreateRequest = new OMBucketCreateRequest(modifiedRequest);

    InetAddress remoteAddress = omBucketCreateRequest.getRemoteAddress();
    UserGroupInformation ugi = omBucketCreateRequest.createUGI();


    // Now check we have original user info and remote address or not.
    // Here from OMRequest user info, converted to UGI and InetAddress.
    Assert.assertEquals(inetAddress.getHostAddress(),
        remoteAddress.getHostAddress());
    Assert.assertEquals(userGroupInformation.getUserName(), ugi.getUserName());
  }
}