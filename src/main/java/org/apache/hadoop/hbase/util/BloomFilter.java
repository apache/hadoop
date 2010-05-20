/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

/**
 * Defines the general behavior of a bloom filter.
 * <p>
 * The Bloom filter is a data structure that was introduced in 1970 and that has been adopted by 
 * the networking research community in the past decade thanks to the bandwidth efficiencies that it
 * offers for the transmission of set membership information between networked hosts.  A sender encodes 
 * the information into a bit vector, the Bloom filter, that is more compact than a conventional 
 * representation. Computation and space costs for construction are linear in the number of elements.  
 * The receiver uses the filter to test whether various elements are members of the set. Though the 
 * filter will occasionally return a false positive, it will never return a false negative. When creating 
 * the filter, the sender can choose its desired point in a trade-off between the false positive rate and the size. 
 * 
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 * 
 * <p>
 * It must be extended in order to define the real behavior.
 */
public interface BloomFilter {
  /** 
   * Allocate memory for the bloom filter data.  Note that bloom data isn't 
   * allocated by default because it can grow large & reads would be better 
   * managed by the LRU cache.
   */
  void allocBloom();
  
  /**
   * Add the specified binary to the bloom filter.
   *
   * @param buf data to be added to the bloom
   */
  void add(byte []buf);

  /**
   * Add the specified binary to the bloom filter.
   *
   * @param buf data to be added to the bloom
   * @param offset offset into the data to be added
   * @param len length of the data to be added
   */
  void add(byte []buf, int offset, int len);

  /**
   * Check if the specified key is contained in the bloom filter.
   *
   * @param buf data to check for existence of
   * @param bloom bloom filter data to search
   * @return true if matched by bloom, false if not
   */
  boolean contains(byte [] buf, ByteBuffer bloom);

  /**
   * Check if the specified key is contained in the bloom filter.
   *
   * @param buf data to check for existence of
   * @param offset offset into the data
   * @param length length of the data
   * @param bloom bloom filter data to search
   * @return true if matched by bloom, false if not
   */
  boolean contains(byte [] buf, int offset, int length, ByteBuffer bloom);
  
  /**
   * @return The number of keys added to the bloom
   */
  int getKeyCount();

  /**
   * @return The max number of keys that can be inserted
   *         to maintain the desired error rate
   */
  public int getMaxKeys();
  
  /**
   * Size of the bloom, in bytes
   */
  public int getByteSize();
  
  /**
   * Finalize the bloom before writing metadata & data to disk
   */
  void finalize();

  /**
   * Get a writable interface into bloom filter meta data.
   * @return writable class
   */
  Writable getMetaWriter();

  /**
   * Get a writable interface into bloom filter data (actual bloom).
   * @return writable class
   */
  Writable getDataWriter();
}
