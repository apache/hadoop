/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Token which holds enough information to continue the key iteration.
 */
public class ContinueToken {

  private String lastKey;

  private String lastDir;

  private static final String CONTINUE_TOKEN_SEPERATOR = "-";

  public ContinueToken(String lastKey, String lastDir) {
    Preconditions.checkNotNull(lastKey,
        "The last key can't be null in the continue token.");
    this.lastKey = lastKey;
    if (lastDir != null && lastDir.length() > 0) {
      this.lastDir = lastDir;
    }
  }

  /**
   * Generate a continuation token which is used in get Bucket.
   *
   * @return if key is not null return continuation token, else returns null.
   */
  public String encodeToString() {
    if (this.lastKey != null) {

      ByteBuffer buffer = ByteBuffer
          .allocate(4 + lastKey.length()
              + (lastDir == null ? 0 : lastDir.length()));
      buffer.putInt(lastKey.length());
      buffer.put(lastKey.getBytes(StandardCharsets.UTF_8));
      if (lastDir != null) {
        buffer.put(lastDir.getBytes(StandardCharsets.UTF_8));
      }

      String hex = Hex.encodeHexString(buffer.array());
      String digest = DigestUtils.sha256Hex(hex);
      return hex + CONTINUE_TOKEN_SEPERATOR + digest;
    } else {
      return null;
    }
  }

  /**
   * Decode a continuation token which is used in get Bucket.
   *
   * @param key
   * @return if key is not null return decoded token, otherwise returns null.
   * @throws OS3Exception
   */
  public static ContinueToken decodeFromString(String key) throws OS3Exception {
    if (key != null) {
      int indexSeparator = key.indexOf(CONTINUE_TOKEN_SEPERATOR);
      if (indexSeparator == -1) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, key);
      }
      String hex = key.substring(0, indexSeparator);
      String digest = key.substring(indexSeparator + 1);
      try {
        checkHash(key, hex, digest);

        ByteBuffer buffer = ByteBuffer.wrap(Hex.decodeHex(hex));
        int keySize = buffer.getInt();

        byte[] actualKeyBytes = new byte[keySize];
        buffer.get(actualKeyBytes);

        byte[] actualDirBytes = new byte[buffer.remaining()];
        buffer.get(actualDirBytes);

        return new ContinueToken(
            new String(actualKeyBytes, StandardCharsets.UTF_8),
            new String(actualDirBytes, StandardCharsets.UTF_8)
        );

      } catch (DecoderException ex) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .INVALID_ARGUMENT, key);
        os3Exception.setErrorMessage("The continuation token provided is " +
            "incorrect");
        throw os3Exception;
      }
    } else {
      return null;
    }
  }

  private static void checkHash(String key, String hex, String digest)
      throws OS3Exception {
    String digestActualKey = DigestUtils.sha256Hex(hex);
    if (!digest.equals(digestActualKey)) {
      OS3Exception ex = S3ErrorTable.newError(S3ErrorTable
          .INVALID_ARGUMENT, key);
      ex.setErrorMessage("The continuation token provided is incorrect");
      throw ex;
    }
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }

  public String getLastDir() {
    return lastDir;
  }

  public void setLastDir(String lastDir) {
    this.lastDir = lastDir;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContinueToken that = (ContinueToken) o;
    return lastKey.equals(that.lastKey) &&
        Objects.equals(lastDir, that.lastDir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastKey);
  }

  @Override
  public String toString() {
    return "ContinueToken{" +
        "lastKey='" + lastKey + '\'' +
        ", lastDir='" + lastDir + '\'' +
        '}';
  }
}
