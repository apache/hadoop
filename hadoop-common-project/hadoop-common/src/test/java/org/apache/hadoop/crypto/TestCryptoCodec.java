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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Longs;

public class TestCryptoCodec {
  private static final Log LOG= LogFactory.getLog(TestCryptoCodec.class);
  private static byte[] key = new byte[16];
  private static byte[] iv = new byte[16];
  private static final int bufferSize = 4096;
  
  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();
  private final String jceCodecClass = 
      "org.apache.hadoop.crypto.JceAesCtrCryptoCodec";
  private final String opensslCodecClass = 
      "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec";
  
  @Before
  public void setUp() throws IOException {
    Random random = new SecureRandom();
    random.nextBytes(key);
    random.nextBytes(iv);
  }

  @Test(timeout=120000)
  public void testJceAesCtrCryptoCodec() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);
    }
    Assert.assertEquals(null, OpensslCipher.getLoadingFailureReason());
    cryptoCodecTest(conf, seed, 0, jceCodecClass, jceCodecClass, iv);
    cryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
    cryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
    // Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
    for(int i = 0; i < 8; i++) {
      iv[8 + i] = (byte) 0xff;
    }
    cryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
    cryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
  }
  
  @Test(timeout=120000)
  public void testOpensslAesCtrCryptoCodec() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);
    }
    Assert.assertEquals(null, OpensslCipher.getLoadingFailureReason());
    cryptoCodecTest(conf, seed, 0, opensslCodecClass, opensslCodecClass, iv);
    cryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
    cryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
    // Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
    for(int i = 0; i < 8; i++) {
      iv[8 + i] = (byte) 0xff;
    }
    cryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
    cryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
  }
  
  private void cryptoCodecTest(Configuration conf, int seed, int count, 
      String encCodecClass, String decCodecClass, byte[] iv) throws IOException, 
      GeneralSecurityException {
    CryptoCodec encCodec = null;
    try {
      encCodec = (CryptoCodec)ReflectionUtils.newInstance(
          conf.getClassByName(encCodecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal crypto codec!");
    }
    LOG.info("Created a Codec object of type: " + encCodecClass);
    
    // Generate data
    DataOutputBuffer data = new DataOutputBuffer();
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for(int i = 0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      
      key.write(data);
      value.write(data);
    }
    LOG.info("Generated " + count + " records");
    
    // Encrypt data
    DataOutputBuffer encryptedDataBuffer = new DataOutputBuffer();
    CryptoOutputStream out = new CryptoOutputStream(encryptedDataBuffer, 
        encCodec, bufferSize, key, iv);
    out.write(data.getData(), 0, data.getLength());
    out.flush();
    out.close();
    LOG.info("Finished encrypting data");
    
    CryptoCodec decCodec = null;
    try {
      decCodec = (CryptoCodec)ReflectionUtils.newInstance(
          conf.getClassByName(decCodecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal crypto codec!");
    }
    LOG.info("Created a Codec object of type: " + decCodecClass);
    
    // Decrypt data
    DataInputBuffer decryptedDataBuffer = new DataInputBuffer();
    decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0, 
        encryptedDataBuffer.getLength());
    CryptoInputStream in = new CryptoInputStream(decryptedDataBuffer, 
        decCodec, bufferSize, key, iv);
    DataInputStream dataIn = new DataInputStream(new BufferedInputStream(in));
    
    // Check
    DataInputBuffer originalData = new DataInputBuffer();
    originalData.reset(data.getData(), 0, data.getLength());
    DataInputStream originalIn = new DataInputStream(
        new BufferedInputStream(originalData));
    
    for(int i=0; i < count; ++i) {
      RandomDatum k1 = new RandomDatum();
      RandomDatum v1 = new RandomDatum();
      k1.readFields(originalIn);
      v1.readFields(originalIn);
      
      RandomDatum k2 = new RandomDatum();
      RandomDatum v2 = new RandomDatum();
      k2.readFields(dataIn);
      v2.readFields(dataIn);
      assertTrue("original and encrypted-then-decrypted-output not equal",
                 k1.equals(k2) && v1.equals(v2));
      
      // original and encrypted-then-decrypted-output have the same hashCode
      Map<RandomDatum, String> m = new HashMap<RandomDatum, String>();
      m.put(k1, k1.toString());
      m.put(v1, v1.toString());
      String result = m.get(k2);
      assertEquals("k1 and k2 hashcode not equal", result, k1.toString());
      result = m.get(v2);
      assertEquals("v1 and v2 hashcode not equal", result, v1.toString());
    }

    // Decrypt data byte-at-a-time
    originalData.reset(data.getData(), 0, data.getLength());
    decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0, 
        encryptedDataBuffer.getLength());
    in = new CryptoInputStream(decryptedDataBuffer, 
        decCodec, bufferSize, key, iv);

    // Check
    originalIn = new DataInputStream(new BufferedInputStream(originalData));
    int expected;
    do {
      expected = originalIn.read();
      assertEquals("Decrypted stream read by byte does not match",
        expected, in.read());
    } while (expected != -1);
    
    // Seek to a certain position and decrypt
    originalData.reset(data.getData(), 0, data.getLength());
    decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0,
        encryptedDataBuffer.getLength());
    in = new CryptoInputStream(new TestCryptoStreams.FakeInputStream(
        decryptedDataBuffer), decCodec, bufferSize, key, iv);
    int seekPos = data.getLength() / 3;
    in.seek(seekPos);
    
    // Check
    TestCryptoStreams.FakeInputStream originalInput = 
        new TestCryptoStreams.FakeInputStream(originalData);
    originalInput.seek(seekPos);
    do {
      expected = originalInput.read();
      assertEquals("Decrypted stream read by byte does not match",
        expected, in.read());
    } while (expected != -1);

    LOG.info("SUCCESS! Completed checking " + count + " records");
    
    // Check secure random generator
    testSecureRandom(encCodec);
  }
  
  /** Test secure random generator */
  private void testSecureRandom(CryptoCodec codec) {
    // len = 16
    checkSecureRandom(codec, 16);
    // len = 32
    checkSecureRandom(codec, 32);
    // len = 128
    checkSecureRandom(codec, 128);
  }
  
  private void checkSecureRandom(CryptoCodec codec, int len) {
    byte[] rand = new byte[len];
    byte[] rand1 = new byte[len];
    codec.generateSecureRandom(rand);
    codec.generateSecureRandom(rand1);
    
    Assert.assertEquals(len, rand.length);
    Assert.assertEquals(len, rand1.length);
    Assert.assertFalse(Arrays.equals(rand, rand1));
  }
  
  /**
   * Regression test for IV calculation, see HADOOP-11343
   */
  @Test(timeout=120000)
  public void testCalculateIV() throws Exception {
    JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
    codec.setConf(conf);

    SecureRandom sr = new SecureRandom();
    byte[] initIV = new byte[16];
    byte[] IV = new byte[16];

    long iterations = 1000;
    long counter = 10000;

    // Overflow test, IV: 00 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff 
    for(int i = 0; i < 8; i++) {
      initIV[8 + i] = (byte)0xff;
    }

    for(long j = 0; j < counter; j++) {
      assertIVCalculation(codec, initIV, j, IV);
    }

    // Random IV and counter sequence test
    for(long i = 0; i < iterations; i++) {
      sr.nextBytes(initIV);

      for(long j = 0; j < counter; j++) {
        assertIVCalculation(codec, initIV, j, IV);
      }
    }

    // Random IV and random counter test
    for(long i = 0; i < iterations; i++) {
      sr.nextBytes(initIV);

      for(long j = 0; j < counter; j++) {
        long c = sr.nextLong();
        assertIVCalculation(codec, initIV, c, IV);
      }
    }
  }

  private void assertIVCalculation(CryptoCodec codec, byte[] initIV,
      long counter, byte[] IV) {
    codec.calculateIV(initIV, counter, IV);

    BigInteger iv = new BigInteger(1, IV);
    BigInteger ref = calculateRef(initIV, counter);

    assertTrue("Calculated IV don't match with the reference", iv.equals(ref));
  }

  private static BigInteger calculateRef(byte[] initIV, long counter) {
    byte[] cb = Longs.toByteArray(counter);
    BigInteger bi = new BigInteger(1, initIV);
    return bi.add(new BigInteger(1, cb));
  }
}
