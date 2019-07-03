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

package org.apache.hadoop.ozone.om.ratis.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeDeleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.io.IOException;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtils {

  private OzoneManagerRatisUtils() {
  }
  /**
   * Create OMClientRequest which enacpsulates the OMRequest.
   * @param omRequest
   * @return OMClientRequest
   * @throws IOException
   */
  public static OMClientRequest createClientRequest(OMRequest omRequest) {
    Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CreateVolume:
      return new OMVolumeCreateRequest(omRequest);
    case SetVolumeProperty:
      boolean hasQuota = omRequest.getSetVolumePropertyRequest()
          .hasQuotaInBytes();
      boolean hasOwner = omRequest.getSetVolumePropertyRequest().hasOwnerName();
      Preconditions.checkState(hasOwner || hasQuota, "Either Quota or owner " +
          "should be set in the SetVolumeProperty request");
      Preconditions.checkState(!(hasOwner && hasQuota), "Either Quota or " +
          "owner should be set in the SetVolumeProperty request. Should not " +
          "set both");
      if (hasQuota) {
        return new OMVolumeSetQuotaRequest(omRequest);
      } else {
        return new OMVolumeSetOwnerRequest(omRequest);
      }
    case DeleteVolume:
      return new OMVolumeDeleteRequest(omRequest);
    case CreateBucket:
      return new OMBucketCreateRequest(omRequest);
    case DeleteBucket:
      return new OMBucketDeleteRequest(omRequest);
    case SetBucketProperty:
      return new OMBucketSetPropertyRequest(omRequest);
    case AllocateBlock:
      return new OMAllocateBlockRequest(omRequest);
    case CreateKey:
      return new OMKeyCreateRequest(omRequest);
    case CommitKey:
      return new OMKeyCommitRequest(omRequest);
    case DeleteKey:
      return new OMKeyDeleteRequest(omRequest);
    case RenameKey:
      return new OMKeyRenameRequest(omRequest);
    case CreateDirectory:
      return new OMDirectoryCreateRequest(omRequest);
    case CreateFile:
      return new OMFileCreateRequest(omRequest);
    default:
      // TODO: will update once all request types are implemented.
      return null;
    }
  }

  /**
   * Convert exception result to {@link OzoneManagerProtocolProtos.Status}.
   * @param exception
   * @return OzoneManagerProtocolProtos.Status
   */
  public static Status exceptionToResponseStatus(IOException exception) {
    if (exception instanceof OMException) {
      return Status.values()[((OMException) exception).getResult().ordinal()];
    } else {
      return Status.INTERNAL_ERROR;
    }
  }
}
