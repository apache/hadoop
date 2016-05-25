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
package org.apache.hadoop.util;

import java.nio.ByteBuffer;

import org.apache.hadoop.fs.ChecksumException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper around JNI support code to do checksum computation
 * natively.
 */
class NativeCrc32 {
  
  /**
   * Return true if the JNI-based native CRC extensions are available.
   */
  public static boolean isAvailable() {
    if (System.getProperty("os.arch").toLowerCase().startsWith("sparc")) {
      return false;
    } else {
      return NativeCodeLoader.isNativeCodeLoaded();
    }
  }

  /**
   * Verify the given buffers of data and checksums, and throw an exception
   * if any checksum is invalid. The buffers given to this function should
   * have their position initially at the start of the data, and their limit
   * set at the end of the data. The position, limit, and mark are not
   * modified.
   * 
   * @param bytesPerSum the chunk size (eg 512 bytes)
   * @param checksumType the DataChecksum type constant (NULL is not supported)
   * @param sums the DirectByteBuffer pointing at the beginning of the
   *             stored checksums
   * @param data the DirectByteBuffer pointing at the beginning of the
   *             data to check
   * @param basePos the position in the file where the data buffer starts 
   * @param fileName the name of the file being verified
   * @throws ChecksumException if there is an invalid checksum
   */
  public static void verifyChunkedSums(int bytesPerSum, int checksumType,
      ByteBuffer sums, ByteBuffer data, String fileName, long basePos)
      throws ChecksumException {
    nativeComputeChunkedSums(bytesPerSum, checksumType,
        sums, sums.position(),
        data, data.position(), data.remaining(),
        fileName, basePos, true);
  }

  public static void verifyChunkedSumsByteArray(int bytesPerSum,
      int checksumType, byte[] sums, int sumsOffset, byte[] data,
      int dataOffset, int dataLength, String fileName, long basePos)
      throws ChecksumException {
    nativeComputeChunkedSumsByteArray(bytesPerSum, checksumType,
        sums, sumsOffset,
        data, dataOffset, dataLength,
        fileName, basePos, true);
  }

  public static void calculateChunkedSums(int bytesPerSum, int checksumType,
      ByteBuffer sums, ByteBuffer data) {
    nativeComputeChunkedSums(bytesPerSum, checksumType,
        sums, sums.position(),
        data, data.position(), data.remaining(),
        "", 0, false);
  }

  public static void calculateChunkedSumsByteArray(int bytesPerSum,
      int checksumType, byte[] sums, int sumsOffset, byte[] data,
      int dataOffset, int dataLength) {
    nativeComputeChunkedSumsByteArray(bytesPerSum, checksumType,
        sums, sumsOffset,
        data, dataOffset, dataLength,
        "", 0, false);
  }

  /**
   * Verify the given buffers of data and checksums, and throw an exception
   * if any checksum is invalid. The buffers given to this function should
   * have their position initially at the start of the data, and their limit
   * set at the end of the data. The position, limit, and mark are not
   * modified.  This method is retained only for backwards-compatibility with
   * prior jar versions that need the corresponding JNI function.
   *
   * @param bytesPerSum the chunk size (eg 512 bytes)
   * @param checksumType the DataChecksum type constant
   * @param sums the DirectByteBuffer pointing at the beginning of the
   *             stored checksums
   * @param sumsOffset start offset in sums buffer
   * @param data the DirectByteBuffer pointing at the beginning of the
   *             data to check
   * @param dataOffset start offset in data buffer
   * @param dataLength length of data buffer
   * @param fileName the name of the file being verified
   * @param basePos the position in the file where the data buffer starts 
   * @throws ChecksumException if there is an invalid checksum
   * @deprecated use {@link #nativeComputeChunkedSums(int, int, ByteBuffer, int,
   *   ByteBuffer, int, int, String, long, boolean)} instead
   */
  @Deprecated
  @VisibleForTesting
  static native void nativeVerifyChunkedSums(
      int bytesPerSum, int checksumType,
      ByteBuffer sums, int sumsOffset,
      ByteBuffer data, int dataOffset, int dataLength,
      String fileName, long basePos) throws ChecksumException;
  
    private static native void nativeComputeChunkedSums(
      int bytesPerSum, int checksumType,
      ByteBuffer sums, int sumsOffset,
      ByteBuffer data, int dataOffset, int dataLength,
      String fileName, long basePos, boolean verify);

    private static native void nativeComputeChunkedSumsByteArray(
      int bytesPerSum, int checksumType,
      byte[] sums, int sumsOffset,
      byte[] data, int dataOffset, int dataLength,
      String fileName, long basePos, boolean verify);

  // Copy the constants over from DataChecksum so that javah will pick them up
  // and make them available in the native code header.
  public static final int CHECKSUM_CRC32 = DataChecksum.CHECKSUM_CRC32;
  public static final int CHECKSUM_CRC32C = DataChecksum.CHECKSUM_CRC32C;
}
