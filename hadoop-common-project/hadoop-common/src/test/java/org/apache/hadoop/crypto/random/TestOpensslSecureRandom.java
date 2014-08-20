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
package org.apache.hadoop.crypto.random;

import java.util.Arrays;

import org.junit.Test;

public class TestOpensslSecureRandom {
  
  @Test(timeout=120000)
  public void testRandomBytes() throws Exception {
    OpensslSecureRandom random = new OpensslSecureRandom();
    
    // len = 16
    checkRandomBytes(random, 16);
    // len = 32
    checkRandomBytes(random, 32);
    // len = 128
    checkRandomBytes(random, 128);
    // len = 256
    checkRandomBytes(random, 256);
  }
  
  /**
   * Test will timeout if secure random implementation always returns a 
   * constant value.
   */
  private void checkRandomBytes(OpensslSecureRandom random, int len) {
    byte[] bytes = new byte[len];
    byte[] bytes1 = new byte[len];
    random.nextBytes(bytes);
    random.nextBytes(bytes1);
    
    while (Arrays.equals(bytes, bytes1)) {
      random.nextBytes(bytes1);
    }
  }
  
  /**
   * Test will timeout if secure random implementation always returns a 
   * constant value.
   */
  @Test(timeout=120000)
  public void testRandomInt() throws Exception {
    OpensslSecureRandom random = new OpensslSecureRandom();
    
    int rand1 = random.nextInt();
    int rand2 = random.nextInt();
    while (rand1 == rand2) {
      rand2 = random.nextInt();
    }
  }
  
  /**
   * Test will timeout if secure random implementation always returns a 
   * constant value.
   */
  @Test(timeout=120000)
  public void testRandomLong() throws Exception {
    OpensslSecureRandom random = new OpensslSecureRandom();
    
    long rand1 = random.nextLong();
    long rand2 = random.nextLong();
    while (rand1 == rand2) {
      rand2 = random.nextLong();
    }
  }
  
  /**
   * Test will timeout if secure random implementation always returns a 
   * constant value.
   */
  @Test(timeout=120000)
  public void testRandomFloat() throws Exception {
    OpensslSecureRandom random = new OpensslSecureRandom();
    
    float rand1 = random.nextFloat();
    float rand2 = random.nextFloat();
    while (rand1 == rand2) {
      rand2 = random.nextFloat();
    }
  }
  
  /**
   * Test will timeout if secure random implementation always returns a 
   * constant value.
   */
  @Test(timeout=120000)
  public void testRandomDouble() throws Exception {
    OpensslSecureRandom random = new OpensslSecureRandom();
    
    double rand1 = random.nextDouble();
    double rand2 = random.nextDouble();
    while (rand1 == rand2) {
      rand2 = random.nextDouble();
    }
  }
}