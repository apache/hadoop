/*
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.io.RawComparator;

/**
 * Common methods Bloom filter methods required at read and write time.
 */
public interface BloomFilterBase {

  /**
   * @return The number of keys added to the bloom
   */
  long getKeyCount();

  /**
   * @return The max number of keys that can be inserted
   *         to maintain the desired error rate
   */
  long getMaxKeys();

  /**
   * @return Size of the bloom, in bytes
   */
  long getByteSize();

  /**
   * Create a key for a row-column Bloom filter.
   */
  byte[] createBloomKey(byte[] rowBuf, int rowOffset, int rowLen,
      byte[] qualBuf, int qualOffset, int qualLen);

  /**
   * @return Bloom key comparator
   */
  RawComparator<byte[]> getComparator();

}
