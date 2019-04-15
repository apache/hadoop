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

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.om.helpers.OmAllocateBlockResponse;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
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

  /**
   * Start Create Bucket Transaction.
   * @param omBucketInfo
   * @return OmBucketInfo
   * @throws IOException
   */
  OmBucketInfo startCreateBucket(OmBucketInfo omBucketInfo) throws IOException;

  /**
   * Apply Create Bucket Changes to OM DB.
   * @param omBucketInfo
   * @throws IOException
   */
  void applyCreateBucket(OmBucketInfo omBucketInfo) throws IOException;

  /**
   * Start Delete Bucket Transaction.
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  void startDeleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Apply Delete Bucket changes to OM DB.
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  void applyDeleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Start SetBucket Property Transaction.
   * @param omBucketArgs
   * @return OmBucketInfo
   * @throws IOException
   */
  OmBucketInfo startSetBucketProperty(OmBucketArgs omBucketArgs)
      throws IOException;

  /**
   * Apply SetBucket Property changes to OM DB.
   * @param omBucketInfo
   * @throws IOException
   */
  void applySetBucketProperty(OmBucketInfo omBucketInfo) throws IOException;

  /**
   * Start Allocate Block Transaction.
   * @param args
   * @param clientID
   * @param excludeList
   * @return OmAllocateBlockResponse
   * @throws IOException
   */
  OmAllocateBlockResponse startAllocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException;

  /**
   * Apply Allocate Block changes to OM DB.
   * @param clientID
   * @param omKeyInfo
   * @throws IOException
   */
  void applyAllocateBlock(long clientID, OmKeyInfo omKeyInfo)
      throws IOException;

  /**
   * Start Open Key Transaction.
   *
   * @param args
   * @return OmAllocateBlockResponse
   * @throws IOException
   */
  OpenKeySession startOpenKey(OmKeyArgs args) throws IOException;

  /**
   * Apply Open Key changes to OM DB.
   * @param omKeyInfo
   * @param keySessionID
   * @throws IOException
   */
  void applyOpenKey(OmKeyInfo omKeyInfo, long keySessionID)
      throws IOException;

  /**
   * Start Rename Key Transaction.
   * @param args
   * @param toKeyName
   * @return OmKeyInfo
   * @throws IOException
   */
  OmKeyInfo startRenameKey(OmKeyArgs args, String toKeyName) throws IOException;

  /**
   * Apply Rename Key changes to OM DB.
   * @param renameKeyInfo
   * @param toKeyName
   * @throws IOException
   */
  void applyRenameKey(OmKeyInfo renameKeyInfo, String toKeyName)
      throws IOException;

  /**
   * Start DeleteKey Transaction.
   * @param omKeyArgs
   * @return OmKeyInfo
   * @throws IOException
   */
  OmKeyInfo startDeleteKey(OmKeyArgs omKeyArgs) throws IOException;

  /**
   * Apply Delete Key changes to OM DB.
   * @param deleteOmKeyInfo
   * @throws IOException
   */
  void applyDeleteKey(OmKeyInfo deleteOmKeyInfo) throws IOException;

  /**
   * Start Commit Key Transaction.
   * @param args
   * @param clientID
   * @return OmKeyInfo
   * @throws IOException
   */
  OmKeyInfo startCommitKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * Apply Commit Key Transaction to OM DB.
   * @param commitKeyInfo
   * @param clientID
   * @throws IOException
   */
  void applyCommitKey(OmKeyInfo commitKeyInfo, long clientID)
      throws IOException;

}
