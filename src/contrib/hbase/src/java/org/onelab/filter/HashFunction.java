/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 
 * (http://www.one-lab.org)
 * 
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

import java.security.*;

/**
 * Implements a hash object that returns a certain number of hashed values.
 * <p>
 * It is based on the SHA-1 algorithm. 
 * 
 * @see org.onelab.filter.Filter The general behavior of a filter
 *
 * @version 1.0 - 2 Feb. 07
 * 
 * @see org.onelab.filter.Key The general behavior of a key being stored in a filter
 * @see org.onelab.filter.Filter The general behavior of a filter
 * 
 * @see <a href="http://www.itl.nist.gov/fipspubs/fip180-1.htm">SHA-1 algorithm</a>
 */
public final class HashFunction{
  /** The SHA-1 algorithm. */
  private MessageDigest sha;

  /** The number of hashed values. */
  private int nbHash;

  /** The maximum highest returned value. */
  private int maxValue;

  /**
   * Constructor.
   * <p>
   * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
   * @param maxValue The maximum highest returned value.
   * @param nbHash The number of resulting hashed values.
   */
  public HashFunction(int maxValue, int nbHash) {
    try {
      sha = MessageDigest.getInstance("SHA-1");
      
    } catch(NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }

    if(maxValue <= 0) {
      throw new IllegalArgumentException("maxValue must be > 0");
    }
    
    if(nbHash <= 0) {
      throw new IllegalArgumentException("nbHash must be > 0");
    }

    this.maxValue = maxValue;
    this.nbHash = nbHash;
  }//end constructor

  /** Clears <i>this</i> hash function. */
  public void clear(){
    sha.reset();
  }//end clear()

  /**
   * Hashes a specified key into several integers.
   * @param k The specified key.
   * @return The array of hashed values.
   */
  @SuppressWarnings("unchecked")
  public int[] hash(Key k){
      byte[] b = k.getBytes();
      if(b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if(b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      sha.update(b);
      byte[] digestBytes = sha.digest();
      int[] result = new int[nbHash];
      int nbBytePerInt = digestBytes.length/nbHash;
      int offset = 0;
      for(int i = 0; i < nbHash; i++){
        int val = 0;
        for(int j = offset; j < offset + nbBytePerInt; j++) {
          val |=
            (digestBytes[offset] & 0xff) << ((nbBytePerInt - 1 - (j - offset)) * 8);
        }
        result[i] = Math.abs(val) % maxValue;
        offset += nbBytePerInt;
      }
      return result;
  }//end hash() 

}//end class
