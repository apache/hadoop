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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumType;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to compute and verify checksums for chunks.
 *
 * This class is not thread safe.
 */
public class Checksum {

  public static final Logger LOG = LoggerFactory.getLogger(Checksum.class);

  private final ChecksumType checksumType;
  private final int bytesPerChecksum;

  private PureJavaCrc32 crc32Checksum;
  private PureJavaCrc32C crc32cChecksum;
  private MessageDigest sha;

  /**
   * Constructs a Checksum object.
   * @param type type of Checksum
   * @param bytesPerChecksum number of bytes of data per checksum
   */
  public Checksum(ChecksumType type, int bytesPerChecksum) {
    this.checksumType = type;
    this.bytesPerChecksum = bytesPerChecksum;
  }

  /**
   * Constructs a Checksum object with default ChecksumType and default
   * BytesPerChecksum.
   */
  @VisibleForTesting
  public Checksum() {
    this.checksumType = ChecksumType.valueOf(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    this.bytesPerChecksum = OzoneConfigKeys
        .OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES; // Default is 1MB
  }

  /**
   * Computes checksum for give data.
   * @param byteBuffer input data in the form of ByteString.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(ByteBuffer byteBuffer)
      throws OzoneChecksumException {
    return computeChecksum(byteBuffer.array(), byteBuffer.position(),
        byteBuffer.limit());
  }

  /**
   * Computes checksum for give data.
   * @param data input data in the form of byte array.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data)
      throws OzoneChecksumException {
    return computeChecksum(data, 0, data.length);
  }

  /**
   * Computes checksum for give data.
   * @param data input data in the form of byte array.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data, int offset, int len)
      throws OzoneChecksumException {
    ChecksumData checksumData = new ChecksumData(this.checksumType, this
        .bytesPerChecksum);
    if (checksumType == ChecksumType.NONE) {
      // Since type is set to NONE, we do not need to compute the checksums
      return checksumData;
    }

    switch (checksumType) {
    case CRC32:
      crc32Checksum = new PureJavaCrc32();
      break;
    case CRC32C:
      crc32cChecksum = new PureJavaCrc32C();
      break;
    case SHA256:
      try {
        sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new OzoneChecksumException(OzoneConsts.FILE_HASH, e);
      }
      break;
    case MD5:
      break;
    default:
      throw new OzoneChecksumException(checksumType);
    }

    // Compute number of checksums needs for given data length based on bytes
    // per checksum.
    int dataSize = len - offset;
    int numChecksums = (dataSize + bytesPerChecksum - 1) / bytesPerChecksum;

    // Checksum is computed for each bytesPerChecksum number of bytes of data
    // starting at offset 0. The last checksum might be computed for the
    // remaining data with length less than bytesPerChecksum.
    List<ByteString> checksumList = new ArrayList<>(numChecksums);
    for (int index = 0; index < numChecksums; index++) {
      checksumList.add(computeChecksumAtIndex(data, index, offset, len));
    }
    checksumData.setChecksums(checksumList);

    return checksumData;
  }

  /**
   * Computes checksum based on checksumType for a data block at given index
   * and a max length of bytesPerChecksum.
   * @param data input data
   * @param index index to compute the offset from where data must be read
   * @param start start pos of the array where the computation has to start
   * @length length of array till which checksum needs to be computed
   * @return computed checksum ByteString
   * @throws OzoneChecksumException thrown when ChecksumType is not recognized
   */
  private ByteString computeChecksumAtIndex(byte[] data, int index, int start,
      int length)
      throws OzoneChecksumException {
    int offset = start + index * bytesPerChecksum;
    int dataLength = length - start;
    int len = bytesPerChecksum;
    if ((offset + len) > dataLength) {
      len = dataLength - offset;
    }
    byte[] checksumBytes = null;
    switch (checksumType) {
    case CRC32:
      checksumBytes = computeCRC32Checksum(data, offset, len);
      break;
    case CRC32C:
      checksumBytes = computeCRC32CChecksum(data, offset, len);
      break;
    case SHA256:
      checksumBytes = computeSHA256Checksum(data, offset, len);
      break;
    case MD5:
      checksumBytes = computeMD5Checksum(data, offset, len);
      break;
    default:
      throw new OzoneChecksumException(checksumType);
    }

    return ByteString.copyFrom(checksumBytes);
  }

  /**
   * Computes CRC32 checksum.
   */
  private byte[] computeCRC32Checksum(byte[] data, int offset, int len) {
    crc32Checksum.reset();
    crc32Checksum.update(data, offset, len);
    return Longs.toByteArray(crc32Checksum.getValue());
  }

  /**
   * Computes CRC32C checksum.
   */
  private byte[] computeCRC32CChecksum(byte[] data, int offset, int len) {
    crc32cChecksum.reset();
    crc32cChecksum.update(data, offset, len);
    return Longs.toByteArray(crc32cChecksum.getValue());
  }

  /**
   * Computes SHA-256 checksum.
   */
  private byte[] computeSHA256Checksum(byte[] data, int offset, int len) {
    sha.reset();
    sha.update(data, offset, len);
    return sha.digest();
  }

  /**
   * Computes MD5 checksum.
   */
  private byte[] computeMD5Checksum(byte[] data, int offset, int len) {
    MD5Hash md5out = MD5Hash.digest(data, offset, len);
    return md5out.getDigest();
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData.
   * @param byteString input data
   * @param checksumData checksumData to match with
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static boolean verifyChecksum(
      ByteString byteString, ChecksumData checksumData)
      throws OzoneChecksumException {
    return verifyChecksum(byteString.toByteArray(), checksumData);
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData.
   * @param data input data
   * @param checksumData checksumData to match with
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static boolean verifyChecksum(byte[] data, ChecksumData checksumData)
      throws OzoneChecksumException {
    ChecksumType checksumType = checksumData.getChecksumType();
    if (checksumType == ChecksumType.NONE) {
      // Checksum is set to NONE. No further verification is required.
      return true;
    }

    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    Checksum checksum = new Checksum(checksumType, bytesPerChecksum);
    ChecksumData computedChecksumData =
        checksum.computeChecksum(data, 0, data.length);

    return checksumData.verifyChecksumDataMatches(computedChecksumData);
  }

  /**
   * Returns a ChecksumData with type NONE for testing.
   */
  @VisibleForTesting
  public static ContainerProtos.ChecksumData getNoChecksumDataProto() {
    return new ChecksumData(ChecksumType.NONE, 0).getProtoBufMessage();
  }
}
