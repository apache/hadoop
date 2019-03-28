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

package org.apache.hadoop.ozone.om.protocol;

import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyLocation;

import java.io.IOException;

/**
 * Protocol to talk to OM HA. These methods are needed only called from
 * OmRequestHandler.
 */
public interface OzoneManagerHAProtocol {

  /**
   * Add a allocate block, it is assumed that the client is having an open
   * key session going on. This block will be appended to this open key session.
   * This will be called only during HA enabled OM, as during HA we get an
   * allocated Block information, and add that information to OM DB.
   *
   * In HA the flow for allocateBlock is in StartTransaction allocateBlock
   * will be called which returns block information, and in the
   * applyTransaction addAllocateBlock will be called to add the block
   * information to DB.
   *
   * @param args the key to append
   * @param clientID the client identification
   * @param keyLocation key location given by allocateBlock
   * @return an allocated block
   * @throws IOException
   */
  OmKeyLocationInfo addAllocatedBlock(OmKeyArgs args, long clientID,
      KeyLocation keyLocation) throws IOException;


  /**
   * Add the openKey entry with given keyInfo and clientID in to openKeyTable.
   * This will be called only from applyTransaction, once after calling
   * applyKey in startTransaction.
   *
   * @param omKeyArgs
   * @param keyInfo
   * @param clientID
   * @throws IOException
   */
  void applyOpenKey(KeyArgs omKeyArgs, KeyInfo keyInfo, long clientID)
      throws IOException;

  /**
   * Initiate multipart upload for the specified key.
   *
   * This will be called only from applyTransaction.
   * @param omKeyArgs
   * @param multipartUploadID
   * @return OmMultipartInfo
   * @throws IOException
   */
  OmMultipartInfo applyInitiateMultipartUpload(OmKeyArgs omKeyArgs,
      String multipartUploadID) throws IOException;

}
