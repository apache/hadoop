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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Iterator for MetaDataStore DB.
 *
 * @param <T>
 */
public interface TableIterator<KEY, T> extends Iterator<T>, Closeable {

  /**
   * seek to first entry.
   */
  void seekToFirst();

  /**
   * seek to last entry.
   */
  void seekToLast();

  /**
   * Seek to the specific key.
   *
   * @param key - Bytes that represent the key.
   * @return VALUE.
   */
  T seek(KEY key) throws IOException;

  /**
   * Returns the key value at the current position.
   * @return KEY
   */
  KEY key() throws IOException;

  /**
   * Returns the VALUE at the current position.
   * @return VALUE
   */
  T value();

}
