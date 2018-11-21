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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.s3.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;


import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

import static org.apache.hadoop.ozone.s3.util.S3Consts
    .RANGE_HEADER_MATCH_PATTERN;
/**
 * Utility class for S3.
 */
@InterfaceAudience.Private
public final class S3utils {

  private S3utils() {

  }
  private static final String CONTINUE_TOKEN_SEPERATOR = "-";

  /**
   * Generate a continuation token which is used in get Bucket.
   * @param key
   * @return if key is not null return continuation token, else returns null.
   */
  public static String generateContinueToken(String key) {
    if (key != null) {
      byte[] byteData = key.getBytes(StandardCharsets.UTF_8);
      String hex = Hex.encodeHexString(byteData);
      String digest = DigestUtils.sha256Hex(key);
      return hex + CONTINUE_TOKEN_SEPERATOR + digest;
    } else {
      return null;
    }
  }

  /**
   * Decode a continuation token which is used in get Bucket.
   * @param key
   * @return if key is not null return decoded token, otherwise returns null.
   * @throws OS3Exception
   */
  public static String decodeContinueToken(String key) throws OS3Exception {
    if (key != null) {
      int indexSeparator = key.indexOf(CONTINUE_TOKEN_SEPERATOR);
      if (indexSeparator == -1) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, key);
      }
      String hex = key.substring(0, indexSeparator);
      String digest = key.substring(indexSeparator + 1);
      try {
        byte[] actualKeyBytes = Hex.decodeHex(hex);
        String digestActualKey = DigestUtils.sha256Hex(actualKeyBytes);
        if (digest.equals(digestActualKey)) {
          return new String(actualKeyBytes, StandardCharsets.UTF_8);
        } else {
          OS3Exception ex = S3ErrorTable.newError(S3ErrorTable
              .INVALID_ARGUMENT, key);
          ex.setErrorMessage("The continuation token provided is incorrect");
          throw ex;
        }
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


  /**
   * Parse the rangeHeader and set the start and end offset.
   * @param rangeHeaderVal
   * @param length
   *
   * @return RangeHeader
   */
  public static RangeHeader parseRangeHeader(String rangeHeaderVal, long
      length) {
    long start = 0;
    long end = 0;
    boolean noStart = false;
    boolean readFull = false;
    boolean inValidRange = false;
    RangeHeader rangeHeader;
    Matcher matcher = RANGE_HEADER_MATCH_PATTERN.matcher(rangeHeaderVal);
    if (matcher.matches()) {
      if (!matcher.group("start").equals("")) {
        start = Integer.parseInt(matcher.group("start"));
      } else {
        noStart = true;
      }
      if (!matcher.group("end").equals("")) {
        end = Integer.parseInt(matcher.group("end"));
      } else {
        end = length - 1;
      }
      if (noStart) {
        if (end < length) {
          start = length - end;
        } else {
          start = 0;
        }
        end = length - 1;
      } else {
        if (start >= length)  {
          readFull = true;
          if (end >= length) {
            inValidRange = true;
          } else {
            start = 0;
            end = length - 1;
          }
        } else {
          if (end >= length) {
            end = length - 1;
          }
        }
      }
    } else {
      // Byte specification is not matching or start and endoffset provided
      // are not matching with regex.
      start = 0;
      end = length - 1;
      readFull = true;
    }
    rangeHeader = new RangeHeader(start, end, readFull, inValidRange);
    return rangeHeader;

  }
}
