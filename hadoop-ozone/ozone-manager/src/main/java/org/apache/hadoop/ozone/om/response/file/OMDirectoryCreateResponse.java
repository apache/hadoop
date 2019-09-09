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

package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Response for create directory request.
 */
public class OMDirectoryCreateResponse extends OMClientResponse {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateResponse.class);
  private OmKeyInfo dirKeyInfo;

  public OMDirectoryCreateResponse(@Nullable OmKeyInfo dirKeyInfo,
      OMResponse omResponse) {
    super(omResponse);
    this.dirKeyInfo = dirKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (dirKeyInfo != null) {
        String dirKey =
            omMetadataManager.getOzoneKey(dirKeyInfo.getVolumeName(),
                dirKeyInfo.getBucketName(), dirKeyInfo.getKeyName());
        omMetadataManager.getKeyTable().putWithBatch(batchOperation, dirKey,
            dirKeyInfo);
      } else {
        // When directory already exists, we don't add it to cache. And it is
        // not an error, in this case dirKeyInfo will be null.
        LOG.debug("Response Status is OK, dirKeyInfo is null in " +
            "OMDirectoryCreateResponse");
      }
    }
  }
}
