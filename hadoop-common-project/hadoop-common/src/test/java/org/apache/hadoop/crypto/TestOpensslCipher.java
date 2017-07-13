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

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;

public class TestOpensslCipher {
  private static final byte[] KEY_16_BYTES = {0x01, 0x02, 0x03, 0x04, 0x05,
      0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16};
  private static final byte[] KEY_24_BYTES = {0x01, 0x02, 0x03, 0x04, 0x05,
      0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
      0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24};
  private static final byte[] KEY_32_BYTES = {0x01, 0x02, 0x03, 0x04, 0x05,
      0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
      0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
      0x30, 0x31, 0x32};
  private static final byte[] KEY_INVALID_BYTES = {0x01, 0x02, 0x03, 0x04,
      0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
      0x17, 0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
      0x29, 0x30, 0x31, 0x32, 0x33};
  private static final byte[] IV = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
      0x07, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  
  @Test(timeout=120000)
  public void testGetInstance() throws Exception {
    Assume.assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    Assert.assertTrue(cipher != null);
    
    try {
      cipher = OpensslCipher.getInstance("AES2/CTR/NoPadding");
      Assert.fail("Should specify correct algorithm.");
    } catch (NoSuchAlgorithmException e) {
      // Expect NoSuchAlgorithmException
    }
    
    try {
      cipher = OpensslCipher.getInstance("AES/CTR/NoPadding2");
      Assert.fail("Should specify correct padding.");
    } catch (NoSuchPaddingException e) {
      // Expect NoSuchPaddingException
    }
  }
  
  @Test(timeout=120000)
  public void testAllKeySizes() throws Exception {
    Assume.assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    // These 3 should work just fine
    cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_16_BYTES, IV);
    cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_24_BYTES, IV);
    cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_32_BYTES, IV);
    // This one should not
    try {
      cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_INVALID_BYTES, IV);
      Assert.fail("Loading an invalid (" + KEY_INVALID_BYTES.length +
                  " bytes) length key should not have worked!");
    } catch (IllegalArgumentException ex) {
      Assert.assertTrue(
                  "Incorrect error msg. for invalid key length test:" +
                          ex.getMessage(),
                  ex.getMessage().contains("Invalid key length")
      );
      Assert.assertTrue(
                  "Key length not in error msg.: " + ex,
                  ex.getMessage().contains(
                          Integer.toString(KEY_INVALID_BYTES.length)));
    }
  }

  @Test(timeout=120000)
  public void testUpdateArguments() throws Exception {
    Assume.assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    Assert.assertTrue(cipher != null);
    
    cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_16_BYTES, IV);
    
    // Require direct buffers
    ByteBuffer input = ByteBuffer.allocate(1024);
    ByteBuffer output = ByteBuffer.allocate(1024);
    
    try {
      cipher.update(input, output);
      Assert.fail("Input and output buffer should be direct buffer.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Direct buffers are required", e);
    }
    
    // Output buffer length should be sufficient to store output data 
    input = ByteBuffer.allocateDirect(1024);
    output = ByteBuffer.allocateDirect(1000);
    try {
      cipher.update(input, output);
      Assert.fail("Output buffer length should be sufficient " +
          "to store output data");
    } catch (ShortBufferException e) {
      GenericTestUtils.assertExceptionContains(
          "Output buffer is not sufficient", e);
    }
  }
  
  @Test(timeout=120000)
  public void testDoFinalArguments() throws Exception {
    Assume.assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    Assert.assertTrue(cipher != null);
    
    cipher.init(OpensslCipher.ENCRYPT_MODE, KEY_16_BYTES, IV);
    
    // Require direct buffer
    ByteBuffer output = ByteBuffer.allocate(1024);
    
    try {
      cipher.doFinal(output);
      Assert.fail("Output buffer should be direct buffer.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Direct buffer is required", e);
    }
  }
}
