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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumType;
import org.apache.hadoop.ozone.OzoneConfigKeys;
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

  private static Function<ByteBuffer, ByteString> newMessageDigestFunction(
      String algorithm) {
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "Failed to get MessageDigest for " + algorithm,  e);
    }
    return data -> {
      md.reset();
      md.update(data);
      return ByteString.copyFrom(md.digest());
    };
  }

  private static ByteString int2ByteString(int n) {
    final ByteString.Output out = ByteString.newOutput();
    try(DataOutputStream dataOut = new DataOutputStream(out)) {
      dataOut.writeInt(n);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to write integer n = " + n + " to a ByteString", e);
    }
    return out.toByteString();
  }

  private static Function<ByteBuffer, ByteString> newChecksumByteBufferFunction(
      Supplier<ChecksumByteBuffer> constructor) {
    final ChecksumByteBuffer algorithm = constructor.get();
    return  data -> {
      algorithm.reset();
      algorithm.update(data);
      return int2ByteString((int)algorithm.getValue());
    };
  }

  /** The algorithms for {@link ChecksumType}. */
  enum Algorithm {
    NONE(() -> data -> ByteString.EMPTY),
    CRC32(() -> newChecksumByteBufferFunction(PureJavaCrc32ByteBuffer::new)),
    CRC32C(() -> newChecksumByteBufferFunction(PureJavaCrc32CByteBuffer::new)),
    SHA256(() -> newMessageDigestFunction("SHA-256")),
    MD5(() -> newMessageDigestFunction("MD5"));

    private final Supplier<Function<ByteBuffer, ByteString>> constructor;

    static Algorithm valueOf(ChecksumType type) {
      return valueOf(type.name());
    }

    Algorithm(Supplier<Function<ByteBuffer, ByteString>> constructor) {
      this.constructor = constructor;
    }

    Function<ByteBuffer, ByteString> newChecksumFunction() {
      return constructor.get();
    }
  }

  private final ChecksumType checksumType;
  private final int bytesPerChecksum;

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
   * @param data input data.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data, int off, int len)
      throws OzoneChecksumException {
    return computeChecksum(ByteBuffer.wrap(data, off, len));
  }

  /**
   * Computes checksum for give data.
   * @param data input data in the form of byte array.
   * @return ChecksumData computed for input data.
   */
  public ChecksumData computeChecksum(byte[] data)
      throws OzoneChecksumException {
    return computeChecksum(ByteBuffer.wrap(data));
  }

  /**
   * Computes checksum for give data.
   * @param data input data.
   * @return ChecksumData computed for input data.
   * @throws OzoneChecksumException thrown when ChecksumType is not recognized
   */
  public ChecksumData computeChecksum(ByteBuffer data)
      throws OzoneChecksumException {
    if (!data.isReadOnly()) {
      data = data.asReadOnlyBuffer();
    }

    final ChecksumData checksumData = new ChecksumData(
        checksumType, bytesPerChecksum);
    if (checksumType == ChecksumType.NONE) {
      // Since type is set to NONE, we do not need to compute the checksums
      return checksumData;
    }

    final Function<ByteBuffer, ByteString> function;
    try {
      function = Algorithm.valueOf(checksumType).newChecksumFunction();
    } catch (Exception e) {
      throw new OzoneChecksumException(checksumType);
    }

    // Compute number of checksums needs for given data length based on bytes
    // per checksum.
    final int dataSize = data.remaining();
    int numChecksums = (dataSize + bytesPerChecksum - 1) / bytesPerChecksum;

    // Checksum is computed for each bytesPerChecksum number of bytes of data
    // starting at offset 0. The last checksum might be computed for the
    // remaining data with length less than bytesPerChecksum.
    List<ByteString> checksumList = new ArrayList<>(numChecksums);
    for (int index = 0; index < numChecksums; index++) {
      checksumList.add(computeChecksum(data, function, bytesPerChecksum));
    }
    checksumData.setChecksums(checksumList);

    return checksumData;
  }

  /**
   * Compute checksum using the algorithm for the data upto the max length.
   * @param data input data
   * @param function the checksum function
   * @param maxLength the max length of data
   * @return computed checksum ByteString
   */
  private static ByteString computeChecksum(ByteBuffer data,
      Function<ByteBuffer, ByteString> function, int maxLength) {
    final int limit = data.limit();
    try {
      final int maxIndex = data.position() + maxLength;
      if (limit > maxIndex) {
        data.limit(maxIndex);
      }
      return function.apply(data);
    } finally {
      data.limit(limit);
    }
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData, starting from index
   * startIndex.
   * @param byteString input data
   * @param checksumData checksumData to match with
   * @param startIndex index of first checksum in checksumData to match with
   *                   data's computed checksum.
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  public static boolean verifyChecksum(ByteString byteString,
      ChecksumData checksumData, int startIndex) throws OzoneChecksumException {
    final ByteBuffer buffer = byteString.asReadOnlyByteBuffer();
    return verifyChecksum(buffer, checksumData, startIndex);
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
    return verifyChecksum(ByteBuffer.wrap(data), checksumData, 0);
  }

  /**
   * Computes the ChecksumData for the input data and verifies that it
   * matches with that of the input checksumData.
   * @param data input data
   * @param checksumData checksumData to match with
   * @param startIndex index of first checksum in checksumData to match with
   *                   data's computed checksum.
   * @throws OzoneChecksumException is thrown if checksums do not match
   */
  private static boolean verifyChecksum(ByteBuffer data,
      ChecksumData checksumData,
      int startIndex) throws OzoneChecksumException {
    ChecksumType checksumType = checksumData.getChecksumType();
    if (checksumType == ChecksumType.NONE) {
      // Checksum is set to NONE. No further verification is required.
      return true;
    }

    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    Checksum checksum = new Checksum(checksumType, bytesPerChecksum);
    final ChecksumData computed = checksum.computeChecksum(data);
    return checksumData.verifyChecksumDataMatches(computed, startIndex);
  }

  /**
   * Returns a ChecksumData with type NONE for testing.
   */
  @VisibleForTesting
  public static ContainerProtos.ChecksumData getNoChecksumDataProto() {
    return new ChecksumData(ChecksumType.NONE, 0).getProtoBufMessage();
  }
}
