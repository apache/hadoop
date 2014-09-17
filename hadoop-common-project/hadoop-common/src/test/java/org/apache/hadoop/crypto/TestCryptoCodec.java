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
import java.security.GeneralSecurityException;
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
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestCryptoCodec {
  private static final Log LOG= LogFactory.getLog(TestCryptoCodec.class);
  private static final byte[] key = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16};
  private static final byte[] iv = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  private static final int bufferSize = 4096;
  
  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();
  private final String jceCodecClass = 
      "org.apache.hadoop.crypto.JceAesCtrCryptoCodec";
  private final String opensslCodecClass = 
      "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec";
  
  @Test(timeout=120000)
  public void testJceAesCtrCryptoCodec() throws Exception {
    if (!"true".equalsIgnoreCase(System.getProperty("runningWithNative"))) {
      LOG.warn("Skipping since test was not run with -Pnative flag");
      Assume.assumeTrue(false);
    }
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);
    }
    Assert.assertEquals(null, OpensslCipher.getLoadingFailureReason());
    cryptoCodecTest(conf, seed, 0, jceCodecClass, jceCodecClass);
    cryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass);
    cryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass);
  }
  
  @Test(timeout=120000)
  public void testOpensslAesCtrCryptoCodec() throws Exception {
    if (!"true".equalsIgnoreCase(System.getProperty("runningWithNative"))) {
      LOG.warn("Skipping since test was not run with -Pnative flag");
      Assume.assumeTrue(false);
    }
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);
    }
    Assert.assertEquals(null, OpensslCipher.getLoadingFailureReason());
    cryptoCodecTest(conf, seed, 0, opensslCodecClass, opensslCodecClass);
    cryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass);
    cryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass);
  }
  
  private void cryptoCodecTest(Configuration conf, int seed, int count, 
      String encCodecClass, String decCodecClass) throws IOException, 
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
}
