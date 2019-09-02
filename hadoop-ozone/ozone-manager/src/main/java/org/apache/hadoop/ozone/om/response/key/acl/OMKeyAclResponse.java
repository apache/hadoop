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

package org.apache.hadoop.ozone.om.response.key.acl;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for Bucket acl request.
 */
public class OMKeyAclResponse extends OMClientResponse {

  private final OmKeyInfo omKeyInfo;

  public OMKeyAclResponse(@Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // If response status is OK and success is true, add to DB batch.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK &&
        getOMResponse().getSuccess()) {
      String dbKey =
          omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
              omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
      omMetadataManager.getKeyTable().putWithBatch(batchOperation,
          dbKey, omKeyInfo);
    }
  }

}

