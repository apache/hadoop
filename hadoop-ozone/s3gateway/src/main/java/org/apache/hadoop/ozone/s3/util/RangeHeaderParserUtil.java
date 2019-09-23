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

import java.util.regex.Matcher;

import org.apache.hadoop.classification.InterfaceAudience;

import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER_MATCH_PATTERN;
/**
 * Utility class for S3.
 */
@InterfaceAudience.Private
public final class RangeHeaderParserUtil {

  private RangeHeaderParserUtil() {
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
