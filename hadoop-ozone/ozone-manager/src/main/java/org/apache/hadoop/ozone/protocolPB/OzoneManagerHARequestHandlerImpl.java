/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Type;

/**
 * Command Handler for OM requests. OM State Machine calls this handler for
 * deserializing the client request and sending it to OM.
 */
public class OzoneManagerHARequestHandlerImpl
    extends OzoneManagerRequestHandler implements OzoneManagerHARequestHandler {

  private OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  public OzoneManagerHARequestHandlerImpl(OzoneManager om,
      OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer) {
    super(om);
    this.ozoneManagerDoubleBuffer = ozoneManagerDoubleBuffer;
  }


  @Override
  public OMResponse handleApplyTransaction(OMRequest omRequest,
      long transactionLogIndex) {
    LOG.debug("Received OMRequest: {}, ", omRequest);
    Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case DeleteBucket:
    case SetBucketProperty:
    case AllocateBlock:
    case CreateKey:
    case CommitKey:
    case DeleteKey:
    case RenameKey:
    case CreateDirectory:
    case CreateFile:
    case PurgeKeys:
    case CreateS3Bucket:
    case DeleteS3Bucket:
    case InitiateMultiPartUpload:
    case CommitMultiPartUpload:
    case AbortMultiPartUpload:
    case CompleteMultiPartUpload:
    case AddAcl:
    case RemoveAcl:
    case SetAcl:
      //TODO: We don't need to pass transactionID, this will be removed when
      // complete write requests is changed to new model. And also we can
      // return OMClientResponse, then adding to doubleBuffer can be taken
      // care by stateMachine. And also integrate both HA and NON HA code
      // paths.
      OMClientRequest omClientRequest =
          OzoneManagerRatisUtils.createClientRequest(omRequest);
      if (omClientRequest != null) {
        OMClientResponse omClientResponse =
            omClientRequest.validateAndUpdateCache(getOzoneManager(),
                transactionLogIndex, ozoneManagerDoubleBuffer::add);
        return omClientResponse.getOMResponse();
      } else {
        //TODO: remove this once we have all HA support for all write request.
        return handle(omRequest);
      }

    default:
      // As all request types are not changed so we need to call handle
      // here.
      return handle(omRequest);
    }
  }
}
