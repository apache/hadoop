/*
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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.model.object.GetFileStatusOutput;
import com.volcengine.tos.model.object.ListedObjectV2;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.object.ChecksumInfo;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class TOSUtils {
  private TOSUtils() {}

  // Checksum header.
  public static final Map<ChecksumType, String> CHECKSUM_HEADER = ImmutableMap.of(
      ChecksumType.CRC32C, "x-tos-hash-crc32c",
      ChecksumType.CRC64ECMA, "x-tos-hash-crc64ecma"
  );

  // Object type header. Object is either 'Appendable' or 'Normal'.
  public static final String OBJECT_TYPE_KEY = "x-tos-object-type";
  public static final String APPENDABLE_TYPE_VALUE = "Appendable";

  // Checksum is magic checksum if the object doesn't support checksum type.
  public static byte[] parseChecksum(Map<String, String> headers, ChecksumInfo checksumInfo) {
    ChecksumType type = checksumInfo.checksumType();
    String header = CHECKSUM_HEADER.get(type);
    if (header == null) {
      return Constants.MAGIC_CHECKSUM;
    }

    String checksumStr = headers.get(header);
    if (checksumStr == null) {
      return Constants.MAGIC_CHECKSUM;
    }

    return parseChecksumStringToBytes(checksumStr, type);
  }

  // Checksum is magic checksum if the object doesn't support checksum type.
  public static byte[] parseChecksum(ListedObjectV2 obj, ChecksumInfo checksumInfo) {
    ChecksumType type = checksumInfo.checksumType();

    String checksumStr;
    if (type == ChecksumType.CRC32C) {
      checksumStr = obj.getHashCrc32c();
    } else if (type == ChecksumType.CRC64ECMA) {
      checksumStr = obj.getHashCrc64ecma();
    } else {
      throw new IllegalArgumentException(
          String.format("Checksum type %s is not supported by TOS.", type.name()));
    }

    if (checksumStr == null) {
      return Constants.MAGIC_CHECKSUM;
    }

    return parseChecksumStringToBytes(checksumStr, type);
  }

  // Checksum is magic checksum if the object doesn't support checksum type.
  public static byte[] parseChecksum(GetFileStatusOutput obj, ChecksumInfo checksumInfo) {
    ChecksumType type = checksumInfo.checksumType();

    if (type == ChecksumType.CRC32C) {
      return parseChecksumStringToBytes(obj.getCrc32(), type);
    } else if (type == ChecksumType.CRC64ECMA) {
      return parseChecksumStringToBytes(obj.getCrc64(), type);
    } else {
      throw new IllegalArgumentException(
          String.format("Checksum type %s is not supported by TOS.", type.name()));
    }
  }

  public static byte[] parseChecksumStringToBytes(String checksum, ChecksumType type) {
    if (checksum == null) {
      return Constants.MAGIC_CHECKSUM;
    }

    switch (type) {
    case CRC32C:
    case CRC64ECMA:
      return Bytes.toBytes(Long.parseUnsignedLong(checksum));
    default:
      throw new IllegalArgumentException(
          String.format("Checksum type %s is not supported by TOS.", type.name()));
    }
  }

  public static String crc64ecma(Map<String, String> headers) {
    String header = CHECKSUM_HEADER.get(ChecksumType.CRC64ECMA);
    return headers.get(header);
  }

  public static boolean appendable(Map<String, String> headers) {
    String value = headers.get(OBJECT_TYPE_KEY);
    return APPENDABLE_TYPE_VALUE.equals(value);
  }
}
