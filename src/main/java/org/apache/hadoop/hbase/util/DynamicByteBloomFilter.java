/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

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

package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

/**
 * Implements a <i>dynamic Bloom filter</i>, as defined in the INFOCOM 2006 paper.
 * <p>
 * A dynamic Bloom filter (DBF) makes use of a <code>s * m</code> bit matrix but
 * each of the <code>s</code> rows is a standard Bloom filter. The creation 
 * process of a DBF is iterative. At the start, the DBF is a <code>1 * m</code>
 * bit matrix, i.e., it is composed of a single standard Bloom filter.
 * It assumes that <code>n<sub>r</sub></code> elements are recorded in the 
 * initial bit vector, where <code>n<sub>r</sub> <= n</code> (<code>n</code> is
 * the cardinality of the set <code>A</code> to record in the filter).  
 * <p>
 * As the size of <code>A</code> grows during the execution of the application,
 * several keys must be inserted in the DBF.  When inserting a key into the DBF,
 * one must first get an active Bloom filter in the matrix.  A Bloom filter is
 * active when the number of recorded keys, <code>n<sub>r</sub></code>, is 
 * strictly less than the current cardinality of <code>A</code>, <code>n</code>.
 * If an active Bloom filter is found, the key is inserted and 
 * <code>n<sub>r</sub></code> is incremented by one. On the other hand, if there
 * is no active Bloom filter, a new one is created (i.e., a new row is added to
 * the matrix) according to the current size of <code>A</code> and the element
 * is added in this new Bloom filter and the <code>n<sub>r</sub></code> value of
 * this new Bloom filter is set to one.  A given key is said to belong to the
 * DBF if the <code>k</code> positions are set to one in one of the matrix rows.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see BloomFilter A Bloom filter
 * 
 * @see <a href="http://www.cse.fau.edu/~jie/research/publications/Publication_files/infocom2006.pdf">Theory and Network Applications of Dynamic Bloom Filters</a>
 */
public class DynamicByteBloomFilter implements BloomFilter {
  /** Current file format version */
  public static final int VERSION = 2;
  /** Maximum number of keys in a dynamic Bloom filter row. */
  protected final int keyInterval;
  /** The maximum false positive rate per bloom */
  protected final float errorRate;
  /** Hash type */
  protected final int hashType;
  /** The number of keys recorded in the current Bloom filter. */
  protected int curKeys;
  /** expected size of bloom filter matrix (used during reads) */
  protected int readMatrixSize;
  /** The matrix of Bloom filters (contains bloom data only during writes). */
  protected ByteBloomFilter[] matrix;

  /**
   * Normal read constructor.  Loads bloom filter meta data.
   * @param meta stored bloom meta data
   * @throws IllegalArgumentException meta data is invalid
   */
  public DynamicByteBloomFilter(ByteBuffer meta) 
  throws IllegalArgumentException { 
    int version = meta.getInt();
    if (version != VERSION) throw new IllegalArgumentException("Bad version");

    this.keyInterval = meta.getInt();
    this.errorRate  = meta.getFloat();
    this.hashType = meta.getInt();
    this.readMatrixSize = meta.getInt();
    this.curKeys = meta.getInt();
    
    readSanityCheck();

    this.matrix = new ByteBloomFilter[1];
    this.matrix[0] = new ByteBloomFilter(keyInterval, errorRate, hashType, 0);
}

  /**
   * Normal write constructor.  Note that this doesn't allocate bloom data by 
   * default.  Instead, call allocBloom() before adding entries.
   * @param bitSize The vector size of <i>this</i> filter.
   * @param functionCount The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @param keyInterval Maximum number of keys to record per Bloom filter row.
   * @throws IllegalArgumentException The input parameters were invalid
   */
  public DynamicByteBloomFilter(int keyInterval, float errorRate, int hashType)
  throws IllegalArgumentException {
    this.keyInterval = keyInterval;
    this.errorRate = errorRate;
    this.hashType = hashType;
    this.curKeys = 0;
    
    if(keyInterval <= 0) {
      throw new IllegalArgumentException("keyCount must be > 0");
    }

    this.matrix = new ByteBloomFilter[1];
    this.matrix[0] = new ByteBloomFilter(keyInterval, errorRate, hashType, 0);
}

  @Override
  public void allocBloom() {
    this.matrix[0].allocBloom();
  }

  void readSanityCheck() throws IllegalArgumentException {
    if (this.curKeys <= 0) {
      throw new IllegalArgumentException("last bloom's key count invalid");
    }

    if (this.readMatrixSize <= 0) {
      throw new IllegalArgumentException("matrix size must be known");
    }
  }
  
  @Override
  public void add(byte []buf, int offset, int len) {
    BloomFilter bf = getCurBloom();

    if (bf == null) {
      addRow();
      bf = matrix[matrix.length - 1];
      curKeys = 0;
    }

    bf.add(buf, offset, len);
    curKeys++;
  }

  @Override
  public void add(byte []buf) {
    add(buf, 0, buf.length);
  }

  /**
   * Should only be used in tests when writing a bloom filter.
   */
  boolean contains(byte [] buf) {
    return contains(buf, 0, buf.length);
  }

  /**
   * Should only be used in tests when writing a bloom filter.
   */
  boolean contains(byte [] buf, int offset, int length) {
    for (int i = 0; i < matrix.length; i++) {
      if (matrix[i].contains(buf, offset, length)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean contains(byte [] buf, ByteBuffer theBloom) {
    return contains(buf, 0, buf.length, theBloom);
  }
  
  @Override
  public boolean contains(byte[] buf, int offset, int length, 
      ByteBuffer theBloom) {
    if(offset + length > buf.length) {
      return false;
    }
    
    // current version assumes uniform size
    int bytesPerBloom = this.matrix[0].getByteSize(); 
   
    if(theBloom.limit() != bytesPerBloom * readMatrixSize) {
      throw new IllegalArgumentException("Bloom does not match expected size");
    }

    ByteBuffer tmp = theBloom.duplicate();

    // note: actually searching an array of blooms that have been serialized
    for (int m = 0; m < readMatrixSize; ++m) {
      tmp.position(m* bytesPerBloom);
      tmp.limit(tmp.position() + bytesPerBloom);
      boolean match = this.matrix[0].contains(buf, offset, length, tmp.slice());
      if (match) {
        return true;
      }
    }
    
    // matched no bloom filters
    return false;
  }

  int bloomCount() {
    return Math.max(this.matrix.length, this.readMatrixSize);
  }

  @Override
  public int getKeyCount() {
    return (bloomCount()-1) * this.keyInterval + this.curKeys;
  }

  @Override
  public int getMaxKeys() {
    return bloomCount() * this.keyInterval;
  }
  
  @Override
  public int getByteSize() {
    return bloomCount() * this.matrix[0].getByteSize();
  }

  @Override
  public void finalize() {
  }

  /**
   * Adds a new row to <i>this</i> dynamic Bloom filter.
   */
  private void addRow() {
    ByteBloomFilter[] tmp = new ByteBloomFilter[matrix.length + 1];

    for (int i = 0; i < matrix.length; i++) {
      tmp[i] = matrix[i];
    }

    tmp[tmp.length-1] = new ByteBloomFilter(keyInterval, errorRate, hashType, 0);
    tmp[tmp.length-1].allocBloom();
    matrix = tmp;
  }

  /**
   * Returns the currently-unfilled row in the dynamic Bloom Filter array.
   * @return BloomFilter The active standard Bloom filter.
   * 			 <code>Null</code> otherwise.
   */
  private BloomFilter getCurBloom() {
    if (curKeys >= keyInterval) {
      return null;
    }

    return matrix[matrix.length - 1];
  }

  @Override
  public Writable getMetaWriter() {
    return new MetaWriter();
  }

  @Override
  public Writable getDataWriter() {
    return new DataWriter();
  }
  
  private class MetaWriter implements Writable {
    protected MetaWriter() {} 
    @Override
    public void readFields(DataInput arg0) throws IOException {
      throw new IOException("Cant read with this class.");
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(VERSION);
      out.writeInt(keyInterval);
      out.writeFloat(errorRate);
      out.writeInt(hashType);
      out.writeInt(matrix.length);
      out.writeInt(curKeys);
    }
  }

  private class DataWriter implements Writable {
    protected DataWriter() {}
    @Override
    public void readFields(DataInput arg0) throws IOException {
      throw new IOException("Cant read with this class.");
    }

    @Override
    public void write(DataOutput out) throws IOException {
      for (int i = 0; i < matrix.length; ++i) {
        matrix[i].writeBloom(out);
      }
    }
  }
}
