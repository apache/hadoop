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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorUtil;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.api.HistorySkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.translator.api.JobMetaData;
import org.apache.hadoop.resourceestimator.translator.api.LogParser;
import org.apache.hadoop.resourceestimator.translator.api.SingleLineParser;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;
import org.apache.hadoop.resourceestimator.translator.validator.ParserValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class to implement {@link LogParser}. It wraps a
 * {@link SingleLineParser} from the {@link Configuration} to parse a log
 * dir/file.
 */
public class BaseLogParser implements LogParser {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BaseLogParser.class);
  private static final ParserValidator INPUT_VALIDATOR = new ParserValidator();
  private SingleLineParser singleLineParser;
  private HistorySkylineStore historySkylineStore;

  @Override public void init(Configuration config,
      HistorySkylineStore skylineStore) throws ResourceEstimatorException {
    singleLineParser = ResourceEstimatorUtil.createProviderInstance(config,
        ResourceEstimatorConfiguration.TRANSLATOR_LINE_PARSER,
        ResourceEstimatorConfiguration.DEFAULT_TRANSLATOR_LINE_PARSER,
        SingleLineParser.class);
    this.historySkylineStore = skylineStore;
  }

  /**
   * Add job's {@link ResourceSkyline}s to the {@link HistorySkylineStore}.
   *
   * @param skylineRecords the {@link Map} which records the completed recurring
   *                       pipeline's {@link ResourceSkyline}s.
   * @throws SkylineStoreException if it failes to addHistory job's
   *     {@link ResourceSkyline}s to the {@link HistorySkylineStore}.
   */
  private void addToSkylineStore(
      final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords)
      throws SkylineStoreException {
    for (final Map.Entry<RecurrenceId, List<ResourceSkyline>> entry :
        skylineRecords.entrySet()) {
      historySkylineStore.addHistory(entry.getKey(), entry.getValue());
    }
  }

  public void parseLine(final String logLine,
      final Map<String, JobMetaData> jobMetas,
      final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords)
      throws DataFieldNotFoundException, ParseException {
    singleLineParser.parseLine(logLine, jobMetas, skylineRecords);
  }

  @Override public final void parseStream(final InputStream logs)
      throws SkylineStoreException, IOException {
    if (!INPUT_VALIDATOR.validate(logs)) {
      LOGGER.error("Input validation fails, please specify with"
          + " valid input parameters.");
      return;
    }
    final Map<RecurrenceId, List<ResourceSkyline>> skylineRecords =
        new HashMap<>();
    final Map<String, JobMetaData> jobMetas =
        new HashMap<String, JobMetaData>();
    final BufferedReader bf = new BufferedReader(new InputStreamReader(logs));
    String line = null;
    while ((line = bf.readLine()) != null) {
      try {
        parseLine(line, jobMetas, skylineRecords);
      } catch (DataFieldNotFoundException e) {
        LOGGER.debug("Data field not found", e);
      } catch (ParseException e) {
        LOGGER.debug("Date conversion error", e);
      }
    }

    addToSkylineStore(skylineRecords);
  }

  /**
   * Release the resource used by the ParserUtil.
   */
  @Override public final void close() {
    historySkylineStore = null;
  }
}
