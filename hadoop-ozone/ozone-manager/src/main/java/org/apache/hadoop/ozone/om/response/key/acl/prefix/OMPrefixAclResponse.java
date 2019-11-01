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

package org.apache.hadoop.ozone.om.response.key.acl.prefix;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Response for Prefix Acl request.
 */
public class OMPrefixAclResponse extends OMClientResponse {
  private final OmPrefixInfo prefixInfo;

  public OMPrefixAclResponse(@Nullable OmPrefixInfo omPrefixInfo,
      @Nonnull OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    this.prefixInfo = omPrefixInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // If response status is OK and success is true, add to DB batch.
    if (getOMResponse().getSuccess()) {
      if ((getOMResponse().hasAddAclResponse()
          && getOMResponse().getAddAclResponse().getResponse()) ||
          (getOMResponse().hasSetAclResponse()
              && getOMResponse().getSetAclResponse().getResponse())) {
        omMetadataManager.getPrefixTable().putWithBatch(batchOperation,
            prefixInfo.getName(), prefixInfo);
      } else if ((getOMResponse().hasRemoveAclResponse()
          && getOMResponse().getRemoveAclResponse().getResponse())) {
        if (prefixInfo.getAcls().size() == 0) {
          // if acl list size is zero delete.
          omMetadataManager.getPrefixTable().deleteWithBatch(batchOperation,
              prefixInfo.getName());
        } else {
          omMetadataManager.getPrefixTable().putWithBatch(batchOperation,
              prefixInfo.getName(), prefixInfo);
        }
      }
    }
  }

}


