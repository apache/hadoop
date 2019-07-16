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

import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

import java.io.IOException;

/**
 * Protocol to talk to OM HA. These methods are needed only called from
 * OmRequestHandler.
 */
public interface OzoneManagerHAProtocol {

  /**
   * Store the snapshot index i.e. the raft log index, corresponding to the
   * last transaction applied to the OM RocksDB, in OM metadata dir on disk.
   * @return the snapshot index
   * @throws IOException
   */
  long saveRatisSnapshot() throws IOException;

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

  /**
   * Start Create Volume Transaction.
   * @param omVolumeArgs
   * @return VolumeList
   * @throws IOException
   */
  VolumeList startCreateVolume(OmVolumeArgs omVolumeArgs) throws IOException;

  /**
   * Apply Create Volume changes to OM DB.
   * @param omVolumeArgs
   * @param volumeList
   * @throws IOException
   */
  void applyCreateVolume(OmVolumeArgs omVolumeArgs,
      VolumeList volumeList) throws IOException;

  /**
   * Start setOwner Transaction.
   * @param volume
   * @param owner
   * @return OmVolumeOwnerChangeResponse
   * @throws IOException
   */
  OmVolumeOwnerChangeResponse startSetOwner(String volume,
      String owner) throws IOException;

  /**
   * Apply Set Quota changes to OM DB.
   * @param oldOwner
   * @param oldOwnerVolumeList
   * @param newOwnerVolumeList
   * @param newOwnerVolumeArgs
   * @throws IOException
   */
  void applySetOwner(String oldOwner, VolumeList oldOwnerVolumeList,
      VolumeList newOwnerVolumeList, OmVolumeArgs newOwnerVolumeArgs)
      throws IOException;

  /**
   * Start Set Quota Transaction.
   * @param volume
   * @param quota
   * @return OmVolumeArgs
   * @throws IOException
   */
  OmVolumeArgs startSetQuota(String volume, long quota) throws IOException;

  /**
   * Apply Set Quota Changes to OM DB.
   * @param omVolumeArgs
   * @throws IOException
   */
  void applySetQuota(OmVolumeArgs omVolumeArgs) throws IOException;

  /**
   * Start Delete Volume Transaction.
   * @param volume
   * @return OmDeleteVolumeResponse
   * @throws IOException
   */
  OmDeleteVolumeResponse startDeleteVolume(String volume) throws IOException;

  /**
   * Apply Delete Volume changes to OM DB.
   * @param volume
   * @param owner
   * @param newVolumeList
   * @throws IOException
   */
  void applyDeleteVolume(String volume, String owner,
      VolumeList newVolumeList) throws IOException;
}
