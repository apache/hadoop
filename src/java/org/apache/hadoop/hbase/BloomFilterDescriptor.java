/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/** 
 * Supplied as a parameter to HColumnDescriptor to specify what kind of
 * bloom filter to use for a column, and its configuration parameters
 */
public class BloomFilterDescriptor implements WritableComparable {
  
  /*
   * Specify the kind of bloom filter that will be instantiated
   */

  /**
   * <i>Bloom filter</i>, as defined by Bloom in 1970.
   */
  public static final int BLOOMFILTER = 1;

  /**
   * <i>counting Bloom filter</i>, as defined by Fan et al. in a ToN 2000 paper.
   */
  public static final int COUNTING_BLOOMFILTER = 2;

  /**
   * <i>retouched Bloom filter</i>, as defined in the CoNEXT 2006 paper.
   */
  public static final int RETOUCHED_BLOOMFILTER = 3;

  /** Default constructor - used in conjunction with Writable */
  public BloomFilterDescriptor() {
    super();
  }
  
  /**
   * @param type The kind of bloom filter to use.
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash functions to consider.
   */
  public BloomFilterDescriptor(int type, int vectorSize, int nbHash) {
    switch(type) {
    case BLOOMFILTER:
    case COUNTING_BLOOMFILTER:
    case RETOUCHED_BLOOMFILTER:
      this.filterType = type;
      break;

    default:
      throw new IllegalArgumentException("Invalid bloom filter type: " + type);
    }
    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
  }
  
  int filterType;
  int vectorSize;
  int nbHash;

  /** {@inheritDoc} */
  @Override
  public String toString() {
    StringBuilder value = new StringBuilder();

    switch(filterType) {
    case BLOOMFILTER:
      value.append("standard");
      break;
    case COUNTING_BLOOMFILTER:
      value.append("counting");
      break;
    case RETOUCHED_BLOOMFILTER:
      value.append("retouched");
    }
    
    value.append("(vector size=");
    value.append(vectorSize);
    value.append(", number hashes=");
    value.append(nbHash);
    value.append(")");
    
    return value.toString();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = Integer.valueOf(this.filterType).hashCode();
    result ^= Integer.valueOf(this.vectorSize).hashCode();
    result ^= Integer.valueOf(this.nbHash).hashCode();
    return result;
  }

  // Writable
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    filterType = in.readInt();
    vectorSize = in.readInt();
    nbHash = in.readInt();
  }
  
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeInt(filterType);
    out.writeInt(vectorSize);
    out.writeInt(nbHash);
  }
  
  // Comparable
  
  /** {@inheritDoc} */
  public int compareTo(Object o) {
    BloomFilterDescriptor other = (BloomFilterDescriptor)o;
    int result = this.filterType - other.filterType;

    if(result == 0) {
      result = this.vectorSize - other.vectorSize;
    }
    
    if(result == 0) {
      result = this.nbHash - other.nbHash;
    }
    return result;
  }
}
