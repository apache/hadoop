/**
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


import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Block Iterator for container. Each container type need to implement this
 * interface.
 * @param <T>
 */
public interface BlockIterator<T> {

  /**
   * This checks if iterator has next element. If it has returns true,
   * otherwise false.
   * @return boolean
   */
  boolean hasNext() throws IOException;

  /**
   * Seek to first entry.
   */
  void seekToFirst();

  /**
   * Seek to last entry.
   */
  void seekToLast();

  /**
   * Get next block in the container.
   * @return next block or null if there are no blocks
   * @throws IOException
   */
  T nextBlock() throws IOException, NoSuchElementException;


}
