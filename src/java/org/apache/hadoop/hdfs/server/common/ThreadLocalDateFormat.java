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
package org.apache.hadoop.hdfs.server.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Thread safe implementation of {@link SimpleDateFormat} 
 * TODO: This needs to be moved to hadoop common project.
 */
public class ThreadLocalDateFormat {
  private final String format;

  /**
   * Constructs {@link ThreadLocalDateFormat} using given date format pattern
   * @param format Date format pattern
   */
  public ThreadLocalDateFormat(String format) {
    this.format = format;
  }

  /**
   * ThreadLocal based {@link SimpleDateFormat}
   */
  private final ThreadLocal<SimpleDateFormat> dateFormat = 
    new ThreadLocal<SimpleDateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        SimpleDateFormat df = new SimpleDateFormat(format);
        return df;
      }
    };

  /**
   * Format given <code>Date</code> into date/time string.
   * @param date Date to be formatted.
   * @return the formatted date-time string.
   */
  public String format(Date date) {
    return dateFormat.get().format(date);
  }

  /**
   * Parse the String to produce <code>Date</code>.
   * @param source String to parse.
   * @return Date parsed from the String.
   * @throws ParseException
   *           - if the beginning of the specified string cannot be parsed.
   */
  public Date parse(String source) throws ParseException {
    return dateFormat.get().parse(source);
  }

  /**
   * @param zone
   */
  public void setTimeZone(TimeZone zone) {
    dateFormat.get().setTimeZone(zone);
  }

  /**
   * Get access to underlying SimpleDateFormat.
   * Note: Do not pass reference to this Date to other threads!
   * @return the SimpleDateFormat for the thread.
   */
  SimpleDateFormat get() {
    return dateFormat.get();
  }
}
