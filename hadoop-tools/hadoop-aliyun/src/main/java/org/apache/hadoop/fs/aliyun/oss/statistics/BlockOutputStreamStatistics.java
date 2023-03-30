/*
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

package org.apache.hadoop.fs.aliyun.oss.statistics;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Block output stream statistics.
 */
@InterfaceStability.Unstable
public interface BlockOutputStreamStatistics {

  /**
   * A block has been allocated.
   */
  void blockAllocated();

  /**
   * A block has been released.
   */
  void blockReleased();

  /**
   * A disk block has been allocated.
   */
  void diskBlockAllocated();

  /**
   * A disk block has been released.
   */
  void diskBlockReleased();

  /**
   * Memory bytes has been allocated.
   * @param size allocated size.
   */
  void bytesAllocated(long size);

  /**
   * Memory bytes has been released.
   * @param size released size.
   */
  void bytesReleased(long size);

  int getBlocksAllocated();

  int getBlocksReleased();

  int getDiskBlocksAllocated();

  int getDiskBlocksReleased();

  long getBytesAllocated();

  long getBytesReleased();
}
