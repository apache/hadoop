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

package org.apache.hadoop.ozone.om.request.key.acl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclResponse;

/**
 * Handle add Acl request for bucket.
 */
public class OMKeySetAclRequest extends OMKeyAclRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyAddAclRequest.class);

  private String path;
  private List<OzoneAclInfo> ozoneAcls;

  public OMKeySetAclRequest(OMRequest omRequest) {
    super(omRequest);
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    path = setAclRequest.getObj().getPath();
    ozoneAcls = setAclRequest.getAclList();
  }

  @Override
  String getPath() {
    return path;
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.SetAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetAclResponse(SetAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMKeyAclResponse(omKeyInfo,
        omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMKeyAclResponse(null,
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception) {
    if (operationResult) {
      LOG.debug("Set acl: {} to path: {} success!", ozoneAcls, path);
    } else {
      if (exception == null) {
        LOG.debug("Set acl {} to path {} failed!", ozoneAcls, path);
      } else {
        LOG.error("Set acl {} to path {} failed!", ozoneAcls, path, exception);
      }
    }
  }

  @Override
  boolean apply(OmKeyInfo omKeyInfo) {
    // No need to check not null here, this will be never called with null.
    return omKeyInfo.setAcls(ozoneAcls);
  }

}

