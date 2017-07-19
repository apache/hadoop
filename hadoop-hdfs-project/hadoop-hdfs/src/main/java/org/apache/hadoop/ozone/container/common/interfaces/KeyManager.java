/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.List;

/**
 * KeyManager deals with Key Operations in the container Level.
 */
public interface KeyManager {
  /**
   * Puts or overwrites a key.
   *
   * @param pipeline - Pipeline.
   * @param data     - Key Data.
   * @throws IOException
   */
  void putKey(Pipeline pipeline, KeyData data) throws IOException;

  /**
   * Gets an existing key.
   *
   * @param data - Key Data.
   * @return Key Data.
   * @throws IOException
   */
  KeyData getKey(KeyData data) throws IOException;

  /**
   * Deletes an existing Key.
   *
   * @param pipeline - Pipeline.
   * @param keyName  Key Data.
   * @throws StorageContainerException
   */
  void deleteKey(Pipeline pipeline, String keyName)
      throws IOException;

  /**
   * List keys in a container.
   *
   * @param pipeline - pipeline.
   * @param prefix   - Prefix in needed.
   * @param startKey  - Key to start from, EMPTY_STRING to begin.
   * @param count    - Number of keys to return.
   * @return List of Keys that match the criteria.
   */
  List<KeyData> listKey(Pipeline pipeline, String prefix, String startKey,
      int count) throws IOException;

  /**
   * Shutdown keyManager.
   */
  void shutdown();
}
