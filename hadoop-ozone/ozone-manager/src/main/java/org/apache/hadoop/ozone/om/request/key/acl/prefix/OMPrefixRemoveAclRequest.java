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

package org.apache.hadoop.ozone.om.request.key.acl.prefix;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManagerImpl.OMPrefixAclOpResult;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.response.key.acl.prefix.OMPrefixAclResponse;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclResponse;

/**
 * Handle add Acl request for prefix.
 */
public class OMPrefixRemoveAclRequest extends OMPrefixAclRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMPrefixAddAclRequest.class);

  private OzoneObj ozoneObj;
  private List<OzoneAcl> ozoneAcls;

  public OMPrefixRemoveAclRequest(OMRequest omRequest) {
    super(omRequest);
    OzoneManagerProtocolProtos.RemoveAclRequest removeAclRequest =
        getOmRequest().getRemoveAclRequest();
    // TODO: conversion of OzoneObj to protobuf can be avoided when we have
    //  single code path for HA and Non-HA
    ozoneObj = OzoneObjInfo.fromProtobuf(removeAclRequest.getObj());
    ozoneAcls = Lists.newArrayList(
        OzoneAcl.fromProtobuf(removeAclRequest.getAcl()));
  }

  @Override
  OzoneObj getOzoneObj() {
    return ozoneObj;
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.RemoveAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmPrefixInfo omPrefixInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setRemoveAclResponse(RemoveAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMPrefixAclResponse(omPrefixInfo,
        omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMPrefixAclResponse(null,
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics) {
    if (operationResult) {
      LOG.debug("Remove acl: {} to path: {} success!", ozoneAcls,
          ozoneObj.getPath());
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.debug("Remove acl {} to path {} failed, because acl does not exist",
            ozoneAcls, ozoneObj.getPath());
      } else {
        LOG.error("Remove acl {} to path {} failed!", ozoneAcls,
            ozoneObj.getPath(), exception);
      }
    }
  }

  @Override
  OMPrefixAclOpResult apply(PrefixManagerImpl prefixManager,
      OmPrefixInfo omPrefixInfo) throws IOException {
    return prefixManager.removeAcl(ozoneObj, ozoneAcls.get(0), omPrefixInfo);
  }

}

