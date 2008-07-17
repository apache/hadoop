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
 * bloom filter to use for a column, and its configuration parameters.
 * 
 * There is no way to automatically determine the vector size and the number of
 * hash functions to use. In particular, bloom filters are very sensitive to the
 * number of elements inserted into them. For HBase, the number of entries
 * depends on the size of the data stored in the column. Currently the default
 * region size is 64MB, so the number of entries is approximately 
 * 64MB / (average value size for column).
 * 
 * If m denotes the number of bits in the Bloom filter (vectorSize),
 * n denotes the number of elements inserted into the Bloom filter and
 * k represents the number of hash functions used (nbHash), then according to
 * Broder and Mitzenmacher,
 * 
 * ( http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/BloomFilterSurvey.pdf )
 * 
 * the probability of false positives is minimized when k is approximately
 * m/n ln(2).
 * 
 */
@Deprecated
public class BloomFilterDescriptor implements WritableComparable {
  private static final double DEFAULT_NUMBER_OF_HASH_FUNCTIONS = 4.0;
  
  /*
   * Specify the kind of bloom filter that will be instantiated
   */

  /** The type of bloom filter */
  public static enum BloomFilterType {
    /** <i>Bloom filter</i>, as defined by Bloom in 1970. */
    BLOOMFILTER,
    /**
     * <i>Counting Bloom filter</i>, as defined by Fan et al. in a ToN 2000 paper.
     */
    COUNTING_BLOOMFILTER,
    /**
     * <i>Retouched Bloom filter</i>, as defined in the CoNEXT 2006 paper.
     */
    RETOUCHED_BLOOMFILTER
  }

  /** Default constructor - used in conjunction with Writable */
  public BloomFilterDescriptor() {
    super();
  }

  /*
   * Constructor.
   * <p>
   * Creates a deep copy of the supplied BloomFilterDescriptor.
   */
  public BloomFilterDescriptor(BloomFilterDescriptor desc) {
    super();
    this.filterType = desc.filterType;
    this.nbHash = desc.nbHash;
    this.vectorSize = desc.vectorSize;
  }

  /**
   * Creates a BloomFilterDescriptor for the specified type of filter, fixes
   * the number of hash functions to 4 and computes a vector size using:
   * 
   * vectorSize = ceil((4 * n) / ln(2))
   * 
   * @param type
   * @param numberOfEntries
   */
  public BloomFilterDescriptor(final BloomFilterType type,
      final int numberOfEntries) {
    
    switch(type) {
    case BLOOMFILTER:
    case COUNTING_BLOOMFILTER:
    case RETOUCHED_BLOOMFILTER:
      this.filterType = type;
      break;

    default:
      throw new IllegalArgumentException("Invalid bloom filter type: " + type);
    }
    this.nbHash = (int) DEFAULT_NUMBER_OF_HASH_FUNCTIONS;
    this.vectorSize = (int) Math.ceil(
        (DEFAULT_NUMBER_OF_HASH_FUNCTIONS * (1.0 * numberOfEntries)) /
        Math.log(2.0));
  }
  
  /**
   * @param type The kind of bloom filter to use.
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash functions to consider.
   */
  public BloomFilterDescriptor(final BloomFilterType type, final int vectorSize,
      final int nbHash) {
    
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
  
  BloomFilterType filterType;
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

  /** @return the vector size */
  public int getVectorSize() {
    return vectorSize;
  }

  /** @return number of hash functions */
  public int getNbHash() {
    return nbHash;
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = this.filterType.hashCode();
    result ^= Integer.valueOf(this.vectorSize).hashCode();
    result ^= Integer.valueOf(this.nbHash).hashCode();
    return result;
  }

  // Writable
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    int ordinal = in.readInt();
    this.filterType = BloomFilterType.values()[ordinal];
    vectorSize = in.readInt();
    nbHash = in.readInt();
  }
  
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeInt(filterType.ordinal());
    out.writeInt(vectorSize);
    out.writeInt(nbHash);
  }
  
  // Comparable
  
  /** {@inheritDoc} */
  public int compareTo(Object o) {
    BloomFilterDescriptor other = (BloomFilterDescriptor)o;
    int result = this.filterType.ordinal() - other.filterType.ordinal();

    if(result == 0) {
      result = this.vectorSize - other.vectorSize;
    }
    
    if(result == 0) {
      result = this.nbHash - other.nbHash;
    }
    return result;
  }
}
