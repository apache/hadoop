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
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeAclOpResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles volume set acl request.
 */
public class OMVolumeSetAclRequest extends OMVolumeAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetAclRequest.class);

  private List<OzoneAcl> ozoneAcls;
  private String volumeName;

  public OMVolumeSetAclRequest(OMRequest omRequest,
      CheckedBiFunction<List<OzoneAcl>, OmVolumeArgs, IOException> aclOp) {
    super(omRequest, aclOp);
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    Preconditions.checkNotNull(setAclRequest);
    ozoneAcls = new ArrayList<>();
    setAclRequest.getAclList().forEach(oai ->
        ozoneAcls.add(OzoneAcl.fromProtobuf(oai)));
    volumeName = setAclRequest.getObj().getPath().substring(1);
  }

  public List<OzoneAcl> getAcls() {
    return ozoneAcls;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public OMClientResponse handleResult(OmVolumeArgs omVolumeArgs,
      OMMetrics omMetrics, IOException exception) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true);

    if (exception == null) {
      OMVolumeSetAclRequest.LOG.debug("Set acls: {} for volume: {}" +
              " success!", getAcls(), volumeName);
      omResponse.setSetAclResponse(OzoneManagerProtocolProtos
          .SetAclResponse.newBuilder().setResponse(true).build());
      return new OMVolumeAclOpResponse(omVolumeArgs, omResponse.build());
    } else {
      omMetrics.incNumVolumeUpdateFails();
      OMVolumeSetAclRequest.LOG.error("Set acls {} for volume {} failed!",
          getAcls(), getVolumeName(), exception);
      return new OMVolumeAclOpResponse(null,
          createErrorOMResponse(omResponse, exception));
    }
  }
}
