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

package org.apache.hadoop.fs.azurebfs.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_CLOCK_SKEW_WITH_SERVER_IN_MS;

public final class DateTimeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss z";

  public static long parseLastModifiedTime(final String lastModifiedTime) {
    long parsedTime = 0;
    try {
      Date utcDate = new SimpleDateFormat(DATE_TIME_PATTERN, Locale.US)
          .parse(lastModifiedTime);
      parsedTime = utcDate.getTime();
    } catch (ParseException e) {
      LOG.error("Failed to parse the date {}", lastModifiedTime);
    } finally {
      return parsedTime;
    }
  }

  /**
   * Tries to identify if an operation was recently executed based on the LMT of
   * a file or folder. LMT needs to be more recent that the original request
   * start time. To include any clock skew with server, LMT within
   * DEFAULT_CLOCK_SKEW_WITH_SERVER_IN_MS from the request start time is going
   * to be considered to qualify for recent operation.
   * @param lastModifiedTime File/Folder LMT
   * @param expectedLMTUpdateTime  original request timestamp which should
   * have updated the LMT on target
   * @return true if the LMT is within timespan for recent operation, else false
   */
  public static boolean isRecentlyModified(final String lastModifiedTime,
      final Instant expectedLMTUpdateTime) {
    long lmtEpochTime = DateTimeUtils.parseLastModifiedTime(lastModifiedTime);
    long currentEpochTime = expectedLMTUpdateTime.toEpochMilli();

    return ((lmtEpochTime > currentEpochTime)
        || ((currentEpochTime - lmtEpochTime) <= DEFAULT_CLOCK_SKEW_WITH_SERVER_IN_MS));
  }

  private DateTimeUtils() {
  }
}
