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
package org.onelab.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.
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
 * contract <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @version 1.0 - 2 Feb. 07
 * 
 * @see org.onelab.filter.Filter The general behavior of a filter
 * 
 * @see <a href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
 */
public class BloomFilter extends Filter {
  /** The bit vector. */
  boolean[] vector;

  /** Default constructor - use with readFields */
  public BloomFilter() {}
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   */
  public BloomFilter(int vectorSize, int nbHash){
    super(vectorSize, nbHash);

    vector = new boolean[this.vectorSize];
  }//end constructor

  @Override
  public void add(Key key) {
    if(key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
      vector[h[i]] = true;
    }
  }//end add()

  @Override
  public void and(Filter filter){
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    BloomFilter bf = (BloomFilter)filter;

    for(int i = 0; i < vectorSize; i++) {
      this.vector[i] &= bf.vector[i];
    }
  }//end and()

  @Override
  public boolean membershipTest(Key key){
    if(key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash.hash(key);
    hash.clear();
    for(int i = 0; i < nbHash; i++) {
      if(!vector[h[i]]) {
        return false;
      }
    }
    return true;
  }//end memberhsipTest()

  @Override
  public void not(){
    for(int i = 0; i < vectorSize; i++) {
      vector[i] = !vector[i];
    }
  }//end not()

  @Override
  public void or(Filter filter){
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    BloomFilter bf = (BloomFilter)filter;

    for(int i = 0; i < vectorSize; i++) {
      this.vector[i] |= bf.vector[i];
    }
  }//end or()

  @Override
  public void xor(Filter filter){
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }

    BloomFilter bf = (BloomFilter)filter;

    for(int i = 0; i < vectorSize; i++) {
      this.vector[i] = (this.vector[i] && !bf.vector[i])
      || (!this.vector[i] && bf.vector[i]);
    }
  }//and xor()

  /** Returns a String representation of <i>this</i> Bloom filter. */
  @Override
  public String toString(){
    StringBuilder res = new StringBuilder();

    for(int i = 0; i < vectorSize; i++) {
      res.append(vector[i] ? "1" : "0");
    }
    return res.toString();
  }//end toString()

  /** Returns a shallow copy of <i>this</i> Bloom filter. */
  @Override
  public Object clone(){
    BloomFilter bf = new BloomFilter(vectorSize, nbHash);
    bf.or(this);
    return bf;
  }//end clone()

  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = super.hashCode();
    for(int i = 0; i < vector.length; i++) {
      result ^= Boolean.valueOf(vector[i]).hashCode();
    }
    return result;
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    for(int i = 0; i < vector.length; i++) {
      out.writeBoolean(vector[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    vector = new boolean[vectorSize];
    for(int i = 0; i < vector.length; i++) {
      vector[i] = in.readBoolean();
    }
  }

  // Comparable
  
  @Override
  public int compareTo(Object o) {
    int result = super.compareTo(o);
    
    BloomFilter other = (BloomFilter)o;
      
    for(int i = 0; result == 0 && i < vector.length; i++) {
      result = (vector[i] == other.vector[i] ? 0
          : (vector[i] ? 1 : -1));
    }
    return result;
  }// end compareTo
}//end class
