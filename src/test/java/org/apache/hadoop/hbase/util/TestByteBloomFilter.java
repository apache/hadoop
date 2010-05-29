/*
 * Copyright 2010 The Apache Software Foundation
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import junit.framework.TestCase;

public class TestByteBloomFilter extends TestCase {
  
  public void testBasicBloom() throws Exception {
    ByteBloomFilter bf1 = new ByteBloomFilter(1000, (float)0.01, Hash.MURMUR_HASH, 0);
    ByteBloomFilter bf2 = new ByteBloomFilter(1000, (float)0.01, Hash.MURMUR_HASH, 0);
    bf1.allocBloom();
    bf2.allocBloom();
    
    // test 1: verify no fundamental false negatives or positives
    byte[] key1 = {1,2,3,4,5,6,7,8,9};
    byte[] key2 = {1,2,3,4,5,6,7,8,7};
    
    bf1.add(key1);
    bf2.add(key2);
    
    assertTrue(bf1.contains(key1));
    assertFalse(bf1.contains(key2));
    assertFalse(bf2.contains(key1));
    assertTrue(bf2.contains(key2));
    
    byte [] bkey = {1,2,3,4};
    byte [] bval = "this is a much larger byte array".getBytes();
    
    bf1.add(bkey);
    bf1.add(bval, 1, bval.length-1);
    
    assertTrue( bf1.contains(bkey) );
    assertTrue( bf1.contains(bval, 1, bval.length-1) );
    assertFalse( bf1.contains(bval) );
    assertFalse( bf1.contains(bval) );
    
    // test 2: serialization & deserialization.  
    // (convert bloom to byte array & read byte array back in as input)
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    bf1.writeBloom(new DataOutputStream(bOut));
    ByteBuffer bb = ByteBuffer.wrap(bOut.toByteArray()); 
    ByteBloomFilter newBf1 = new ByteBloomFilter(1000, (float)0.01,
        Hash.MURMUR_HASH, 0);
    assertTrue(newBf1.contains(key1, bb));
    assertFalse(newBf1.contains(key2, bb));
    assertTrue( newBf1.contains(bkey, bb) );
    assertTrue( newBf1.contains(bval, 1, bval.length-1, bb) );
    assertFalse( newBf1.contains(bval, bb) );
    assertFalse( newBf1.contains(bval, bb) );
    
    System.out.println("Serialized as " + bOut.size() + " bytes");
    assertTrue(bOut.size() - bf1.byteSize < 10); //... allow small padding
  }
  
  public void testBloomFold() throws Exception {
    // test: foldFactor < log(max/actual)
    ByteBloomFilter b = new ByteBloomFilter(1003, (float)0.01, Hash.MURMUR_HASH, 2);
    b.allocBloom();
    int origSize = b.getByteSize();
    assertEquals(1204, origSize);
    for (int i = 0; i < 12; ++i) {
      b.add(Bytes.toBytes(i));
    }
    b.finalize();
    assertEquals(origSize>>2, b.getByteSize());
    int falsePositives = 0;
    for (int i = 0; i < 25; ++i) {
      if (b.contains(Bytes.toBytes(i))) {
        if(i >= 12) falsePositives++;
      } else {
        assertFalse(i < 12);
      }
    }
    assertTrue(falsePositives <= 1);

    // test: foldFactor > log(max/actual)
  }

  public void testBloomPerf() throws Exception {
    // add
    float err = (float)0.01;
    ByteBloomFilter b = new ByteBloomFilter(10*1000*1000, (float)err, Hash.MURMUR_HASH, 3);
    b.allocBloom();
    long startTime =  System.currentTimeMillis();
    int origSize = b.getByteSize();
    for (int i = 0; i < 1*1000*1000; ++i) {
      b.add(Bytes.toBytes(i));
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Total Add time = " + (endTime - startTime) + "ms");

    // fold
    startTime = System.currentTimeMillis();
    b.finalize();
    endTime = System.currentTimeMillis();
    System.out.println("Total Fold time = " + (endTime - startTime) + "ms");
    assertTrue(origSize >= b.getByteSize()<<3);
    
    // test
    startTime = System.currentTimeMillis();
    int falsePositives = 0;
    for (int i = 0; i < 2*1000*1000; ++i) {
      
      if (b.contains(Bytes.toBytes(i))) {
        if(i >= 1*1000*1000) falsePositives++;
      } else {
        assertFalse(i < 1*1000*1000);
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Total Contains time = " + (endTime - startTime) + "ms");
    System.out.println("False Positive = " + falsePositives);
    assertTrue(falsePositives <= (1*1000*1000)*err);

    // test: foldFactor > log(max/actual)
  }

  public void testDynamicBloom() throws Exception {
    int keyInterval = 1000;
    float err = (float)0.01;
    BitSet valid = new BitSet(keyInterval*4);

    DynamicByteBloomFilter bf1 = new DynamicByteBloomFilter(keyInterval, err,
        Hash.MURMUR_HASH);
    bf1.allocBloom();
    
    long seed = System.currentTimeMillis();
    Random r = new Random(seed);
    System.out.println("seed = " + seed);
    
    for (int i = 0; i < keyInterval*4; ++i) { // add
      if (r.nextBoolean()) {
        bf1.add(Bytes.toBytes(i));
        valid.set(i);
        
        // we assume only 2 blooms in this test, so exit before a 3rd is made
        if (bf1.getKeyCount() == 2000) {
          break;
        }
      }
    }
    assertTrue(2 <= bf1.bloomCount());
    System.out.println("keys added = " + bf1.getKeyCount());

    // test serialization/deserialization
    ByteArrayOutputStream metaOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    bf1.getMetaWriter().write(new DataOutputStream(metaOut));
    bf1.getDataWriter().write(new DataOutputStream(dataOut));
    ByteBuffer bb = ByteBuffer.wrap(dataOut.toByteArray()); 
    DynamicByteBloomFilter newBf1 = new DynamicByteBloomFilter(
        ByteBuffer.wrap(metaOut.toByteArray()));

    int falsePositives = 0;
    for (int i = 0; i < keyInterval*4; ++i) { // check
      if (newBf1.contains(Bytes.toBytes(i), bb)) {
        if (!valid.get(i)) ++falsePositives;
      } else {
        if (valid.get(i)) {
          assert false;
        }
      }
    }
    
    // Dynamic Blooms are a little sneaky.  The error rate currently isn't 
    // 'err', it's err * bloomCount.  bloomCount == 2000/1000 == 2 in this case
    // So, the actual error rate should be roughly: 
    //    (keyInterval*2) * err * bloomCount
    // allow some tolerance
    System.out.println("False positives: " + falsePositives);
    assertTrue(falsePositives <= (keyInterval*5)*err); 
  }

}
