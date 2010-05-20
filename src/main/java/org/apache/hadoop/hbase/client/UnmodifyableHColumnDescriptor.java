/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 * Immutable HColumnDescriptor
 */
public class UnmodifyableHColumnDescriptor extends HColumnDescriptor {

  /**
   * @param desc wrapped
   */
  public UnmodifyableHColumnDescriptor (final HColumnDescriptor desc) {
    super(desc);
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setValue(byte[], byte[])
   */
  @Override
  public void setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setValue(java.lang.String, java.lang.String)
   */
  @Override
  public void setValue(String key, String value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setMaxVersions(int)
   */
  @Override
  public void setMaxVersions(int maxVersions) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setInMemory(boolean)
   */
  @Override
  public void setInMemory(boolean inMemory) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setBlockCacheEnabled(boolean)
   */
  @Override
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setTimeToLive(int)
   */
  @Override
  public void setTimeToLive(int timeToLive) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setCompressionType(org.apache.hadoop.hbase.io.hfile.Compression.Algorithm)
   */
  @Override
  public void setCompressionType(Compression.Algorithm type) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }
}