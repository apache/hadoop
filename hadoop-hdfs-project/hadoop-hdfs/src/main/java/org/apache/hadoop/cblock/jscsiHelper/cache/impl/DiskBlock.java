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
package org.apache.hadoop.cblock.jscsiHelper.cache.impl;

import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import java.nio.ByteBuffer;

/**
 * Impl class for LogicalBlock.
 */
public class DiskBlock implements LogicalBlock {
  private ByteBuffer data;
  private long blockID;
  private boolean persisted;

  /**
   * Constructs a DiskBlock Class from the following params.
   * @param blockID - 64-bit block ID
   * @param data - Byte Array
   * @param persisted - Flag which tells us if this is persisted to remote
   */
  public DiskBlock(long blockID, byte[] data, boolean persisted) {
    if (data !=null) {
      this.data = ByteBuffer.wrap(data);
    }
    this.blockID = blockID;
    this.persisted = persisted;
  }

  @Override
  public ByteBuffer getData() {
    return data;
  }

  /**
   * Frees the byte buffer since we don't need it any more.
   */
  @Override
  public void clearData() {
    data.clear();
  }

  @Override
  public long getBlockID() {
    return blockID;
  }

  @Override
  public boolean isPersisted() {
    return persisted;
  }

  /**
   * Sets the value of persisted.
   * @param value - True if this has been persisted to container, false
   * otherwise.
   */
  public void setPersisted(boolean value) {
    persisted = value;
  }

}