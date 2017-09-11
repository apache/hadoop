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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;

import java.io.IOException;
import java.util.List;

/**
 * Handles key level commands.
 */
public interface KeyManager {

  /**
   * Start key manager.
   */
  void start();

  /**
   * Stop key manager.
   */
  void stop() throws IOException;

  /**
   * Given the args of a key to put, return a pipeline for the key. Writes
   * the key to pipeline mapping to meta data.
   *
   * Note that this call only allocate a block for key, and adds the
   * corresponding entry to metadata. The block will be returned to client side
   * handler DistributedStorageHandler. Which will make another call to
   * datanode to create container (if needed) and writes the key.
   *
   * In case that the container creation or key write failed on
   * DistributedStorageHandler, this key's metadata will still stay in KSM.
   *
   * @param args the args of the key provided by client.
   * @return a KsmKeyInfo instance client uses to talk to container.
   * @throws Exception
   */
  KsmKeyInfo allocateKey(KsmKeyArgs args) throws IOException;

  /**
   * Look up an existing key. Return the info of the key to client side, which
   * DistributedStorageHandler will use to access the data on datanode.
   *
   * @param args the args of the key provided by client.
   * @return a KsmKeyInfo instance client uses to talk to container.
   * @throws IOException
   */
  KsmKeyInfo lookupKey(KsmKeyArgs args) throws IOException;

  /**
   * Deletes an object by an object key. The key will be immediately removed
   * from KSM namespace and become invisible to clients. The object data
   * will be removed in async manner that might retain for some time.
   *
   * @param args the args of the key provided by client.
   * @throws IOException if specified key doesn't exist or
   * some other I/O errors while deleting an object.
   */
  void deleteKey(KsmKeyArgs args) throws IOException;

  /**
   * Returns a list of keys represented by {@link KsmKeyInfo}
   * in the given bucket.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   *   This key is excluded from the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<KsmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns a list of pending deletion key info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the
   * key name and all its associated block IDs. A pending deletion key is
   * stored with #deleting# prefix in KSM DB.
   *
   * @param count max number of keys to return.
   * @return a list of {@link BlockGroup} representing keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionKeys(int count) throws IOException;

  /**
   * Deletes a pending deletion key by its name. This is often called when
   * key can be safely deleted from this layer. Once called, all footprints
   * of the key will be purged from KSM DB.
   *
   * @param objectKeyName object key name with #deleting# prefix.
   * @throws IOException if specified key doesn't exist or other I/O errors.
   */
  void deletePendingDeletionKey(String objectKeyName) throws IOException;
}
