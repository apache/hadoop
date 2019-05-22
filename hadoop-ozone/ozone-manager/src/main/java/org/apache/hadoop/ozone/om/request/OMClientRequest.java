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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.net.InetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * OMClientRequest provides methods which every write OM request should
 * implement.
 */
public abstract class OMClientRequest {

  private OMRequest omRequest;

  public OMClientRequest(OMRequest omRequest) {
    Preconditions.checkNotNull(omRequest);
    this.omRequest = omRequest;
  }
  /**
   * Perform pre-execute steps on a OMRequest.
   *
   * Called from the RPC context, and generates a OMRequest object which has
   * all the information that will be either persisted
   * in RocksDB or returned to the caller once this operation
   * is executed.
   *
   * @return OMRequest that will be serialized and handed off to Ratis for
   *         consensus.
   */
  public abstract OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException;

  /**
   * Validate the OMRequest and update the cache.
   * This step should verify that the request can be executed, perform
   * any authorization steps and update the in-memory cache.

   * This step does not persist the changes to the database.
   *
   * @return the response that will be returned to the client.
   */
  public abstract OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex);

  @VisibleForTesting
  public OMRequest getOmRequest() {
    return omRequest;
  }

  /**
   * Get UGI information from the OMRequest.
   * @return
   */
  public OzoneManagerProtocolProtos.UgiInfo getUgiInfo() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteAddress = ProtobufRpcEngine.Server.getRemoteIp();
    OzoneManagerProtocolProtos.UgiInfo.Builder ugi =
        OzoneManagerProtocolProtos.UgiInfo.newBuilder();

    if (user != null) {
      ugi.setUserName(user.getUserName());
    }

    if (remoteAddress != null) {
      ugi.setRemoteAddress(remoteAddress.getHostAddress()).build();
    }

    return ugi.build();
  }

  /**
   * Check Acls of ozone object.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @throws IOException
   */
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key) throws IOException {
    ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
        createUGI(), InetAddress.getByName(omRequest.getUgiInfo()
            .getRemoteAddress()));
  }

  private UserGroupInformation createUGI() {
    return UserGroupInformation.createRemoteUser(
        omRequest.getUgiInfo().getUserName());
  }

}
