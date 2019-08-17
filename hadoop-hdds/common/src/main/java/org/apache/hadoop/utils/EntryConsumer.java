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

package org.apache.hadoop.utils;

import java.io.IOException;

/**
 * A consumer for metadata store key-value entries.
 * Used by {@link MetadataStore} class.
 */
@FunctionalInterface
public interface EntryConsumer {

  /**
   * Consumes a key and value and produces a boolean result.
   * @param key key
   * @param value value
   * @return a boolean value produced by the consumer
   * @throws IOException
   */
  boolean consume(byte[] key, byte[] value) throws IOException;
}
