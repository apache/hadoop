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
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_CLOCK_SKEW_WITH_SERVER_IN_MS;

public final class DateTimeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss z";

  /**
   * Lenient ISO-8601 parser that accepts a local date-time with an optional
   * trailing offset (for example {@code 2026-07-06T10:31:19} or
   * {@code 2026-07-06T10:31:19Z}). Immutable and thread-safe, so it is shared to
   * avoid per-call allocation on the ListBlobs parsing hot path.
   */
  private static final DateTimeFormatter ISO_FLEXIBLE_PARSER =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .optionalStart().appendOffsetId().optionalEnd()
          .toFormatter(Locale.US);

  /**
   * RFC 1123 GMT formatter matching {@link #DATE_TIME_PATTERN} (two-digit day,
   * literal {@code GMT}) so normalized Arrow timestamps are byte-for-byte
   * identical to the XML ListBlobs values. Immutable and thread-safe.
   */
  private static final DateTimeFormatter RFC_1123_GMT_FORMATTER =
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
          .withZone(ZoneOffset.UTC);

  /** RFC 1123 three-letter weekday names indexed by Sakamoto value (0 = Sun). */
  private static final String[] RFC_1123_WEEKDAYS =
      {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

  /** RFC 1123 three-letter month names indexed by (month - 1). */
  private static final String[] RFC_1123_MONTHS =
      {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

  /** Minimum length of a canonical ISO local date-time ({@code yyyy-MM-ddTHH:mm:ss}). */
  private static final int ISO_LOCAL_DATE_TIME_LENGTH = 19;

  // Field offsets and widths within a canonical ISO local date-time
  // ({@code yyyy-MM-ddTHH:mm:ss}) used by the fast positional parser.
  private static final int ISO_YEAR_OFFSET = 0;
  private static final int ISO_YEAR_WIDTH = 4;
  private static final int ISO_MONTH_OFFSET = 5;
  private static final int ISO_DAY_OFFSET = 8;
  private static final int ISO_HOUR_OFFSET = 11;
  private static final int ISO_MINUTE_OFFSET = 14;
  private static final int ISO_SECOND_OFFSET = 17;
  private static final int ISO_FIELD_WIDTH = 2;
  private static final int ISO_DASH_YEAR_MONTH_POS = 4;
  private static final int ISO_DASH_MONTH_DAY_POS = 7;
  private static final int ISO_DATE_TIME_SEP_POS = 10;
  private static final int ISO_COLON_HOUR_MINUTE_POS = 13;
  private static final int ISO_COLON_MINUTE_SECOND_POS = 16;

  // Inclusive bounds for validating the extracted date-time fields.
  private static final int MAX_MONTH = 12;
  private static final int MAX_DAY = 31;
  private static final int MAX_HOUR = 23;
  private static final int MAX_MINUTE = 59;
  private static final int MAX_SECOND = 60;

  // Rendering helpers.
  private static final int RFC_1123_BUFFER_LENGTH = 29;
  private static final int DECIMAL_RADIX = 10;
  private static final int MAX_DECIMAL_DIGIT = 9;

  // Constants for Sakamoto's day-of-week algorithm.
  private static final int GREGORIAN_CENTURY = 100;
  private static final int GREGORIAN_LEAP_CYCLE = 400;
  private static final int LEAP_YEAR_DIVISOR = 4;
  private static final int DAYS_IN_WEEK = 7;

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
   * Normalizes an ISO-8601 date-time string returned by the Arrow (Photon)
   * ListBlobs response into the RFC 1123 GMT representation used by the XML
   * ListBlobs response (for example {@code Mon, 06 Jul 2026 10:31:19 GMT}).
   * This keeps the Photon path byte-for-byte compatible with the XML path so
   * that {@link #parseLastModifiedTime(String)} and downstream FileStatus
   * conversion behave identically for both response formats.
   * <p>
   * Blob storage timestamps are in UTC; Arrow serializes them without an
   * explicit zone (for example {@code 2026-07-06T10:31:19}), so values without
   * a zone are interpreted as UTC. If the value is null, empty, or cannot be
   * parsed it is returned unchanged.
   *
   * @param arrowDateTime the ISO-8601 date-time string from the Arrow response
   * @return the equivalent RFC 1123 GMT date-time string, or the original
   * value when it is null, empty, or not parseable
   */
  public static String formatArrowDateTimeToRfc1123(final String arrowDateTime) {
    if (arrowDateTime == null || arrowDateTime.isEmpty()) {
      return arrowDateTime;
    }
    // Fast path for the canonical fixed-width ISO UTC form that Photon emits;
    // avoids the java.time parse/format cost on the per-row hot path.
    String fast = fastIsoUtcToRfc1123(arrowDateTime);
    if (fast != null) {
      return fast;
    }
    Instant instant = parseToInstant(arrowDateTime.trim());
    if (instant == null) {
      return arrowDateTime;
    }
    return RFC_1123_GMT_FORMATTER.format(instant);
  }

  /**
   * Converts a canonical ISO-8601 UTC local date-time
   * ({@code yyyy-MM-ddTHH:mm:ss}, optionally with fractional seconds and/or a
   * trailing {@code Z}) directly to the RFC 1123 GMT string using integer field
   * extraction and Sakamoto's day-of-week algorithm. This is allocation-light
   * and never throws, so it keeps the ListBlobs parsing hot path fast.
   * <p>
   * Returns {@code null} for anything it cannot handle confidently (for example
   * a value carrying an explicit non-UTC offset, or a non-ISO string), letting
   * the caller fall back to the precise {@link #parseToInstant(String)} path.
   *
   * @param value the candidate ISO-8601 date-time string
   * @return the RFC 1123 GMT string, or {@code null} to signal fallback
   */
  private static String fastIsoUtcToRfc1123(final String value) {
    int length = value.length();
    if (length < ISO_LOCAL_DATE_TIME_LENGTH) {
      return null;
    }
    if (value.charAt(ISO_DASH_YEAR_MONTH_POS) != '-'
        || value.charAt(ISO_DASH_MONTH_DAY_POS) != '-'
        || value.charAt(ISO_DATE_TIME_SEP_POS) != 'T'
        || value.charAt(ISO_COLON_HOUR_MINUTE_POS) != ':'
        || value.charAt(ISO_COLON_MINUTE_SECOND_POS) != ':'
        || !isUtcRemainder(value, length)) {
      return null;
    }
    int year = parseDigits(value, ISO_YEAR_OFFSET, ISO_YEAR_WIDTH);
    int month = parseDigits(value, ISO_MONTH_OFFSET, ISO_FIELD_WIDTH);
    int day = parseDigits(value, ISO_DAY_OFFSET, ISO_FIELD_WIDTH);
    int hour = parseDigits(value, ISO_HOUR_OFFSET, ISO_FIELD_WIDTH);
    int minute = parseDigits(value, ISO_MINUTE_OFFSET, ISO_FIELD_WIDTH);
    int second = parseDigits(value, ISO_SECOND_OFFSET, ISO_FIELD_WIDTH);
    if (year < 0 || month < 1 || month > MAX_MONTH || day < 1 || day > MAX_DAY
        || hour < 0 || hour > MAX_HOUR || minute < 0 || minute > MAX_MINUTE
        || second < 0 || second > MAX_SECOND) {
      return null;
    }
    StringBuilder sb = new StringBuilder(RFC_1123_BUFFER_LENGTH);
    sb.append(RFC_1123_WEEKDAYS[sakamotoDayOfWeek(year, month, day)])
        .append(", ");
    appendTwoDigits(sb, day);
    sb.append(' ').append(RFC_1123_MONTHS[month - 1]).append(' ')
        .append(year).append(' ');
    appendTwoDigits(sb, hour);
    sb.append(':');
    appendTwoDigits(sb, minute);
    sb.append(':');
    appendTwoDigits(sb, second);
    sb.append(" GMT");
    return sb.toString();
  }

  /**
   * Returns true when the portion of {@code value} after the seconds field
   * denotes UTC: empty, a fractional-seconds part, and/or a trailing {@code Z}.
   * An explicit numeric offset returns false so the caller uses the slow path.
   */
  private static boolean isUtcRemainder(final String value, final int length) {
    int i = ISO_LOCAL_DATE_TIME_LENGTH;
    if (i < length && value.charAt(i) == '.') {
      i++;
      int fractionStart = i;
      while (i < length && value.charAt(i) >= '0' && value.charAt(i) <= '9') {
        i++;
      }
      if (i == fractionStart) {
        return false;
      }
    }
    if (i == length) {
      return true;
    }
    return i == length - 1
        && (value.charAt(i) == 'Z' || value.charAt(i) == 'z');
  }

  /**
   * Parses {@code count} decimal digits of {@code value} starting at
   * {@code start}, or returns {@code -1} if any character is not a digit.
   */
  private static int parseDigits(final String value, final int start,
      final int count) {
    int result = 0;
    for (int i = start; i < start + count; i++) {
      int digit = value.charAt(i) - '0';
      if (digit < 0 || digit > MAX_DECIMAL_DIGIT) {
        return -1;
      }
      result = result * DECIMAL_RADIX + digit;
    }
    return result;
  }

  private static void appendTwoDigits(final StringBuilder sb, final int value) {
    if (value < DECIMAL_RADIX) {
      sb.append('0');
    }
    sb.append(value);
  }

  /**
   * Sakamoto's algorithm for the day of week. Returns 0 for Sunday through 6
   * for Saturday, matching {@link #RFC_1123_WEEKDAYS}.
   */
  private static int sakamotoDayOfWeek(final int year, final int month,
      final int day) {
    final int[] monthOffset = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
    int y = month < 3 ? year - 1 : year;
    return (y + y / LEAP_YEAR_DIVISOR - y / GREGORIAN_CENTURY
        + y / GREGORIAN_LEAP_CYCLE + monthOffset[month - 1] + day) % DAYS_IN_WEEK;
  }

  /**
   * Parses an ISO-8601 date-time string into an {@link Instant}, accepting
   * values with a trailing {@code Z}, an explicit offset, or no zone at all
   * (interpreted as UTC). The common no-zone case is handled without throwing
   * so this stays cheap on the ListBlobs parsing hot path.
   *
   * @param value the trimmed ISO-8601 date-time string
   * @return the parsed {@link Instant}, or {@code null} if it cannot be parsed
   */
  private static Instant parseToInstant(final String value) {
    try {
      TemporalAccessor parsed = ISO_FLEXIBLE_PARSER.parseBest(
          value, OffsetDateTime::from, LocalDateTime::from);
      if (parsed instanceof OffsetDateTime) {
        return ((OffsetDateTime) parsed).toInstant();
      }
      return ((LocalDateTime) parsed).toInstant(ZoneOffset.UTC);
    } catch (DateTimeException e) {
      LOG.error("Failed to parse Arrow date-time {}", value, e);
      return null;
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
