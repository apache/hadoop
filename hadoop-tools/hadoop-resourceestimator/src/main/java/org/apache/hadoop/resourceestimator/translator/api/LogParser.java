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

package org.apache.hadoop.resourceestimator.translator.api;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.api.HistorySkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;

/**
 * LogParser iterates over a stream of logs, uses {@link SingleLineParser} to
 * parse each line, and adds extracted {@code ResourceSkyline}s to the
 * {@code SkylineStore}.
 */
public interface LogParser extends AutoCloseable {

  /**
   * Initializing the LogParser, including loading solver parameters from
   * configuration file.
   *
   * @param config       {@link Configuration} for the LogParser.
   * @param skylineStore the {@link HistorySkylineStore} which stores recurring
   *                     pipeline's {@code
   *                     ResourceSkyline}s.
   * @throws ResourceEstimatorException if initialization of a
   *     {@code SingleLineParser} fails.
   */
  void init(Configuration config, HistorySkylineStore skylineStore)
      throws ResourceEstimatorException;

  /**
   * Parses each line in the log stream, and adds extracted
   * {@code ResourceSkyline}s to the {@code
   * SkylineStore}.
   *
   * @param logs the stream of input logs.
   * @throws SkylineStoreException if it fails to addHistory extracted
   *     {@code ResourceSkyline}s to the {@code SkylineStore}.
   * @throws IOException if it fails to read from the {@link InputStream}.
   */
  void parseStream(InputStream logs) throws SkylineStoreException, IOException;

  @Override void close();
}
