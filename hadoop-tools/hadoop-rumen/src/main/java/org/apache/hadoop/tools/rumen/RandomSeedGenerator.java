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
package org.apache.hadoop.tools.rumen;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to generate new random seeds from a master
 * seed. This is needed to make the Random().next*() calls in rumen and mumak
 * deterministic so that mumak simulations become deterministically replayable.
 *
 * In these tools we need many independent streams of random numbers, some of
 * which are created dynamically. We seed these streams with the sub-seeds 
 * returned by RandomSeedGenerator.
 * 
 * For a slightly more complicated approach to generating multiple streams of 
 * random numbers with better theoretical guarantees, see
 * P. L'Ecuyer, R. Simard, E. J. Chen, and W. D. Kelton, 
 * ``An Objected-Oriented Random-Number Package with Many Long Streams and 
 * Substreams'', Operations Research, 50, 6 (2002), 1073--1075
 * http://www.iro.umontreal.ca/~lecuyer/papers.html
 * http://www.iro.umontreal.ca/~lecuyer/myftp/streams00/
 */
public class RandomSeedGenerator {
  private static Logger LOG = LoggerFactory.getLogger(RandomSeedGenerator.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  
  /** MD5 algorithm instance, one for each thread. */
  private static final ThreadLocal<MessageDigest> md5Holder =
      new ThreadLocal<MessageDigest>() {
        @Override protected MessageDigest initialValue() {
          MessageDigest md5 = null; 
          try {
            md5 = MessageDigest.getInstance("MD5");
          } catch (NoSuchAlgorithmException nsae) {
            throw new RuntimeException("Can't create MD5 digests", nsae);
          }
          return md5;
        }
      };
      
  /**
   * Generates a new random seed.
   *
   * @param streamId a string identifying the stream of random numbers
   * @param masterSeed higher level master random seed
   * @return the random seed. Different (streamId, masterSeed) pairs result in
   *         (vastly) different random seeds.
   */   
  public static long getSeed(String streamId, long masterSeed) {
    MessageDigest md5 = md5Holder.get();
    md5.reset();
    //'/' : make sure that we don't get the same str from ('11',0) and ('1',10)
    // We could have fed the bytes of masterSeed one by one to md5.update()
    // instead
    String str = streamId + '/' + masterSeed;
    byte[] digest = md5.digest(str.getBytes(UTF_8));
    // Create a long from the first 8 bytes of the digest
    // This is fine as MD5 has the avalanche property.
    // Paranoids could have XOR folded the other 8 bytes in too. 
    long seed = 0;
    for (int i=0; i<8; i++) {
      seed = (seed<<8) + ((int)digest[i]+128);
    }
    return seed;
  }
}
