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
package org.onelab.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements a <i>counting Bloom filter</i>, as defined by Fan et al. in a ToN
 * 2000 paper.
 * <p>
 * A counting Bloom filter is an improvement to standard a Bloom filter as it
 * allows dynamic additions and deletions of set membership information.  This 
 * is achieved through the use of a counting vector instead of a bit vector.
 * 
 * contract <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @version 1.0 - 5 Feb. 07
 * 
 * @see org.onelab.filter.Filter The general behavior of a filter
 * 
 * @see <a href="http://portal.acm.org/citation.cfm?id=343571.343572">Summary cache: a scalable wide-area web cache sharing protocol</a>
 */
public final class CountingBloomFilter extends Filter {
  /** Counter vector. */
  private byte[] vector;

  /** Default constructor - use with readFields */
  public CountingBloomFilter() {}
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   */
  public CountingBloomFilter(int vectorSize, int nbHash){
    super(vectorSize, nbHash);
    vector = new byte[vectorSize];
  }//end constructor

  /** {@inheritDoc} */
  @Override
  public void add(Key key) {
    if(key == null) {
      throw new NullPointerException("key can not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
      vector[h[i]]++;
    }
  }//end add()

  /**
   * Removes a specified key from <i>this</i> counting Bloom filter.
   * <p>
   * <b>Invariant</b>: nothing happens if the specified key does not belong to <i>this</i> counter Bloom filter.
   * @param key The key to remove.
   */
  public void delete(Key key) {
    if(key == null) {
      throw new NullPointerException("Key may not be null");
    }
    if(!membershipTest(key)) {
      throw new IllegalArgumentException("Key is not a member");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
      if(vector[h[i]] >= 1) {
        vector[h[i]]--;
      }
    }
  }//end delete

  /** {@inheritDoc} */
  @Override
  public void and(Filter filter){
    if(filter == null
        || !(filter instanceof CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }
    CountingBloomFilter cbf = (CountingBloomFilter)filter;

    for(int i = 0; i < vectorSize; i++) {
      this.vector[i] &= cbf.vector[i];
    }
  }//end and()

  /** {@inheritDoc} */
  @Override
  public boolean membershipTest(Key key){
    if(key == null) {
      throw new NullPointerException("Key may not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
      if(vector[h[i]] == 0) {
        return false;
      }
    }

    return true;
  }//end membershipTest()

  /** {@inheritDoc} */
  @Override
  public void not(){
    throw new UnsupportedOperationException("not() is undefined for "
        + this.getClass().getName());
  }//end not()

  /** {@inheritDoc} */
  @Override
  public void or(Filter filter){
    if(filter == null
        || !(filter instanceof CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    CountingBloomFilter cbf = (CountingBloomFilter)filter;

    for(int i = 0; i < vectorSize; i++) {
      this.vector[i] |= cbf.vector[i];
    }
  }//end or()

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unused")
  public void xor(Filter filter){
    throw new UnsupportedOperationException("xor() is undefined for "
        + this.getClass().getName());
  }//end xor()

  /** {@inheritDoc} */
  @Override
  public String toString(){
    StringBuilder res = new StringBuilder();

    for(int i = 0; i < vectorSize; i++) {
      if(i > 0) {
        res.append(" ");
      }
      res.append(vector[i]&0xff);
    }

    return res.toString();
  }//end toString()

  /** {@inheritDoc} */
  @Override
  public Object clone(){
    CountingBloomFilter cbf = new CountingBloomFilter(vectorSize, nbHash);
    cbf.or(this);
    return cbf;
  }//end clone()

  // Writable

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    for(int i = 0; i < vector.length; i++) {
      out.writeByte(vector[i]);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    vector = new byte[vectorSize];
    for(int i = 0; i < vector.length; i++) {
      vector[i] = in.readByte();
    }
  }
}//end class
