/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.translator.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.translator.api.LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utility functions for {@link LogParser}.
 */
public class LogParserUtil {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LogParserUtil.class);
  private LogParser logParser;
  private DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

  /**
   * Set the {@link LogParser} to use.
   *
   * @param logParser the {@link LogParser} to use.
   */
  public void setLogParser(final LogParser logParser) {
    this.logParser = logParser;
  }

  /**
   * Set date format for the {@link LogParser}.
   *
   * @param datePattern the date pattern in the log.
   */
  public void setDateFormat(final String datePattern) {
    this.format = new SimpleDateFormat(datePattern);
  }

  /**
   * Converts String date to unix timestamp. Note that we assume the time in the
   * logs has the same time zone with the machine which runs the
   * {@link RmSingleLineParser}.
   *
   * @param date The String date.
   * @return Unix time stamp.
   * @throws ParseException if data conversion from String to unix timestamp
   *                        fails.
   */
  public long stringToUnixTimestamp(final String date) throws ParseException {
    return format.parse(date).getTime();
  }

  /**
   * Parse the log file/directory.
   *
   * @param logFile the file/directory of the log.
   * @throws SkylineStoreException      if fails to addHistory to
   *                                    {@link SkylineStore}.
   * @throws IOException                if fails to parse the log.
   * @throws ResourceEstimatorException if the {@link LogParser}
   *     is not initialized.
   */
  public final void parseLog(final String logFile)
      throws SkylineStoreException, IOException, ResourceEstimatorException {
    if (logParser == null) {
      throw new ResourceEstimatorException("The log parser is not initialized,"
          + " please try again after initializing.");
    }
    InputStream inputStream = null;
    try {
      inputStream = new FileInputStream(logFile);
      logParser.parseStream(inputStream);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }
}
