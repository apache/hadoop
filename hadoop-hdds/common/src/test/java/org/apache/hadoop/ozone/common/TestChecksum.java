/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Checksum} class.
 */
public class TestChecksum {

  private static final int BYTES_PER_CHECKSUM = 10;
  private static final ContainerProtos.ChecksumType CHECKSUM_TYPE_DEFAULT =
      ContainerProtos.ChecksumType.SHA256;

  private Checksum getChecksum(ContainerProtos.ChecksumType type) {
    if (type == null) {
      type = CHECKSUM_TYPE_DEFAULT;
    }
    return new Checksum(type, BYTES_PER_CHECKSUM);
  }

  /**
   * Tests {@link Checksum#verifyChecksum(byte[], ChecksumData)}.
   */
  @Test
  public void testVerifyChecksum() throws Exception {
    Checksum checksum = getChecksum(null);
    int dataLen = 55;
    byte[] data = RandomStringUtils.randomAlphabetic(dataLen).getBytes();

    ChecksumData checksumData = checksum.computeChecksum(data);

    // A checksum is calculate for each bytesPerChecksum number of bytes in
    // the data. Since that value is 10 here and the data length is 55, we
    // should have 6 checksums in checksumData.
    Assert.assertEquals(6, checksumData.getChecksums().size());

    // Checksum verification should pass
    Assert.assertTrue("Checksum mismatch",
        Checksum.verifyChecksum(data, checksumData));
  }

  /**
   * Tests that if data is modified, then the checksums should not match.
   */
  @Test
  public void testIncorrectChecksum() throws Exception {
    Checksum checksum = getChecksum(null);
    byte[] data = RandomStringUtils.randomAlphabetic(55).getBytes();
    ChecksumData originalChecksumData = checksum.computeChecksum(data);

    // Change the data and check if new checksum matches the original checksum.
    // Modifying one byte of data should be enough for the checksum data to
    // mismatch
    data[50] = (byte) (data[50]+1);
    ChecksumData newChecksumData = checksum.computeChecksum(data);
    Assert.assertNotEquals("Checksums should not match for different data",
        originalChecksumData, newChecksumData);
  }

  /**
   * Tests that checksum calculated using two different checksumTypes should
   * not match.
   */
  @Test
  public void testChecksumMismatchForDifferentChecksumTypes() throws Exception {
    byte[] data = RandomStringUtils.randomAlphabetic(55).getBytes();

    // Checksum1 of type SHA-256
    Checksum checksum1 = getChecksum(null);
    ChecksumData checksumData1 = checksum1.computeChecksum(data);

    // Checksum2 of type CRC32
    Checksum checksum2 = getChecksum(ContainerProtos.ChecksumType.CRC32);
    ChecksumData checksumData2 = checksum2.computeChecksum(data);

    // The two checksums should not match as they have different types
    Assert.assertNotEquals(
        "Checksums should not match for different checksum types",
        checksum1, checksum2);
  }
}
