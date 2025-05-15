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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestOpensslCipher {
  private static final byte[] key = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16};
  private static final byte[] iv = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  
  @Test
  @Timeout(value = 120)
  public void testGetInstance() throws Exception {
    assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    assertTrue(cipher != null);
    
    try {
      cipher = OpensslCipher.getInstance("AES2/CTR/NoPadding");
      fail("Should specify correct algorithm.");
    } catch (NoSuchAlgorithmException e) {
      // Expect NoSuchAlgorithmException
    }
    
    try {
      cipher = OpensslCipher.getInstance("AES/CTR/NoPadding2");
      fail("Should specify correct padding.");
    } catch (NoSuchPaddingException e) {
      // Expect NoSuchPaddingException
    }
  }
  
  @Test
  @Timeout(value = 120)
  public void testUpdateArguments() throws Exception {
    assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    assertTrue(cipher != null);
    
    cipher.init(OpensslCipher.ENCRYPT_MODE, key, iv);
    
    // Require direct buffers
    ByteBuffer input = ByteBuffer.allocate(1024);
    ByteBuffer output = ByteBuffer.allocate(1024);
    
    try {
      cipher.update(input, output);
      fail("Input and output buffer should be direct buffer.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Direct buffers are required", e);
    }
    
    // Output buffer length should be sufficient to store output data 
    input = ByteBuffer.allocateDirect(1024);
    output = ByteBuffer.allocateDirect(1000);
    try {
      cipher.update(input, output);
      fail("Output buffer length should be sufficient to store output data");
    } catch (ShortBufferException e) {
      GenericTestUtils.assertExceptionContains(
          "Output buffer is not sufficient", e);
    }
  }
  
  @Test
  @Timeout(value = 120)
  public void testDoFinalArguments() throws Exception {
    assumeTrue(OpensslCipher.getLoadingFailureReason() == null);
    OpensslCipher cipher = OpensslCipher.getInstance("AES/CTR/NoPadding");
    assertTrue(cipher != null);
    
    cipher.init(OpensslCipher.ENCRYPT_MODE, key, iv);
    
    // Require direct buffer
    ByteBuffer output = ByteBuffer.allocate(1024);
    
    try {
      cipher.doFinal(output);
      fail("Output buffer should be direct buffer.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Direct buffer is required", e);
    }
  }

  @Test
  @Timeout(value = 120)
  public void testIsSupportedSuite() throws Exception {
    assumeTrue(OpensslCipher.getLoadingFailureReason() == null,
        "Skipping due to failure of loading OpensslCipher.");
    assertFalse(OpensslCipher.isSupported(CipherSuite.UNKNOWN),
        "Unknown suite must not be supported.");
    assertTrue(OpensslCipher.isSupported(CipherSuite.AES_CTR_NOPADDING),
        "AES/CTR/NoPadding is not an optional suite.");
  }
}
