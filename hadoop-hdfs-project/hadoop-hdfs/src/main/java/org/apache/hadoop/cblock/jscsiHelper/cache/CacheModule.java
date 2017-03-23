/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.cblock.jscsiHelper.cache;

import java.io.IOException;

/**
 * Defines the interface for cache implementations. The cache will be called
 * by cblock storage module when it performs IO operations.
 */
public interface CacheModule {
  /**
   * check if the key is cached, if yes, returned the cached object.
   * otherwise, load from data source. Then put it into cache.
   *
   * @param blockID
   * @return the target block.
   */
  LogicalBlock get(long blockID) throws IOException;

  /**
   * put the value of the key into cache.
   * @param blockID
   * @param value
   */
  void put(long blockID, byte[] value) throws IOException;

  void flush() throws IOException;

  void start() throws IOException;

  void stop() throws IOException;

  void close() throws IOException;

  boolean isDirtyCache();
}
