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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * KSM metadata manager interface.
 */
public interface MetadataManager {
  /**
   * Start metadata manager.
   */
  void start();

  /**
   * Stop metadata manager.
   */
  void stop() throws IOException;

  /**
   * Returns the read lock used on Metadata DB.
   * @return readLock
   */
  Lock readLock();

  /**
   * Returns the write lock used on Metadata DB.
   * @return writeLock
   */
  Lock writeLock();

  /**
   * Returns the value associated with this key.
   * @param key - key
   * @return value
   */
  byte[] get(byte[] key);

  /**
   * Puts a Key into Metadata DB.
   * @param key   - key
   * @param value - value
   */
  void put(byte[] key, byte[] value);

  /**
   * Performs a batch Put to Metadata DB.
   * Can be used to do multiple puts atomically.
   * @param list - list of Map.Entry
   */
  void batchPut(List<Map.Entry<byte[], byte[]>> list) throws IOException;

}
