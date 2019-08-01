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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.scm.storage.CheckedBiFunction;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeAclOpResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Handles volume remove acl request.
 */
public class OMVolumeRemoveAclRequest extends OMVolumeAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeRemoveAclRequest.class);

  private static CheckedBiFunction<List<OzoneAcl>,
      OmVolumeArgs, IOException> volumeRemoveAclOp;

  static {
    volumeRemoveAclOp = (acls, volArgs) -> volArgs.removeAcl(acls.get(0));
  }

  private List<OzoneAcl> ozoneAcls;
  private String volumeName;

  public OMVolumeRemoveAclRequest(OMRequest omRequest) {
    super(omRequest, volumeRemoveAclOp);
    OzoneManagerProtocolProtos.RemoveAclRequest removeAclRequest =
        getOmRequest().getRemoveAclRequest();
    Preconditions.checkNotNull(removeAclRequest);
    ozoneAcls = Lists.newArrayList(
        OzoneAcl.fromProtobuf(removeAclRequest.getAcl()));
    volumeName = removeAclRequest.getObj().getPath().substring(1);
  }

  @Override
  public List<OzoneAcl> getAcls() {
    return ozoneAcls;
  }

  @Override
  public String getVolumeName() {
    return volumeName;
  }

  private OzoneAcl getAcl() {
    return ozoneAcls.get(0);
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setStatus(OzoneManagerProtocolProtos.Status.OK).setSuccess(true);
  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmVolumeArgs omVolumeArgs, boolean result){
    omResponse.setRemoveAclResponse(OzoneManagerProtocolProtos.RemoveAclResponse
        .newBuilder().setResponse(result).build());
    return new OMVolumeAclOpResponse(omVolumeArgs, omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException ex) {
    return new OMVolumeAclOpResponse(null,
        createErrorOMResponse(omResponse, ex));
  }

  @Override
  void onComplete(IOException ex) {
    if (ex == null) {
      LOG.debug("Remove acl: {} from volume: {} success!",
          getAcl(), getVolumeName());
    } else {
      LOG.error("Remove acl {} from volume {} failed!",
          getAcl(), getVolumeName(), ex);
    }
  }
}
