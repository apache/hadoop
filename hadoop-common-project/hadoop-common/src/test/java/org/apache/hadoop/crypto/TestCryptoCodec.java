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
package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCryptoCodec {
  private static CryptoCodec codec;
  
  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new Configuration();
    codec = CryptoCodec.getInstance(conf);
  }
  
  @AfterClass
  public static void shutdown() throws Exception {
  }
  
  @Test(timeout=120000)
  public void testSecureRandom() throws Exception {
    // len = 16
    checkSecureRandom(16);
    
    // len = 32
    checkSecureRandom(32);
    
    // len = 128
    checkSecureRandom(128);
  }
  
  private void checkSecureRandom(int len) {
    byte[] rand = codec.generateSecureRandom(len);
    byte[] rand1 = codec.generateSecureRandom(len);
    
    Assert.assertEquals(len, rand.length);
    Assert.assertEquals(len, rand1.length);
    Assert.assertFalse(bytesArrayEquals(rand, rand1));
  }
  
  private boolean bytesArrayEquals(byte[] expected, byte[] actual) {
    if ((expected == null  && actual != null) || 
        (expected != null && actual == null)) {
      return false;
    }
    if (expected == null && actual == null) {
      return true;
    }
    
    if (expected.length != actual.length) {
      return false;
    }
    
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        return false;
      }
    }
    
    return true;
  }
}
