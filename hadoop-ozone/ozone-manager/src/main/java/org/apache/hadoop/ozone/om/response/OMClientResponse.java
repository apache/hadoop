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

package org.apache.hadoop.ozone.om.response;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Interface for OM Responses, each OM response should implement this interface.
 */
public abstract class OMClientResponse {

  private OMResponse omResponse;

  public OMClientResponse(OMResponse omResponse) {
    Preconditions.checkNotNull(omResponse);
    this.omResponse = omResponse;
  }

  /**
   * Implement logic to add the response to batch.
   * @param omMetadataManager
   * @param batchOperation
   * @throws IOException
   */
  public abstract void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException;

  /**
   * Return OMResponse.
   * @return OMResponse
   */
  public OMResponse getOMResponse() {
    return omResponse;
  }


  /**
   * For Non-HA we cannot use double buffer and add response to cache and
   * then return response to the client. This method helps the response to be
   * persisted to OM DB in Non-HA.
   *
   * Why in Non-HA current double buffer cannot be used is, we return
   * response to the client once the response is added to double buffer. And
   * if flush to disk is missed from double buffer, Ratis provides guaranty
   * to replay transactions from last Applied transaction Index. We don't
   * have similar thing in Non-HA, so current implementation of double buffer
   * cannot be used.
   */
  public abstract void addResponseToOMDB(OMMetadataManager omMetadataManager)
      throws IOException;

}

