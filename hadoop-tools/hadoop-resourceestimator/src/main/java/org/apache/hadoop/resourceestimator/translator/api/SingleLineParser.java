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

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;

/**
 * SingleLineParser parses one line in the log file, extracts the
 * {@link ResourceSkyline}s and stores them.
 */
public interface SingleLineParser {
  /**
   * Parse one line in the log file, extract the {@link ResourceSkyline}s and
   * store them.
   *
   * @param logLine        one line in the log file.
   * @param jobMetas       the job metadata collected during parsing.
   * @param skylineRecords the valid {@link ResourceSkyline}s extracted from the
   *                       log.
   * @throws DataFieldNotFoundException if certain data fields are not found in
   *                                    the log.
   * @throws ParseException if it fails to convert date string to
   *     unix timestamp successfully.
   */
  void parseLine(String logLine, Map<String, JobMetaData> jobMetas,
      Map<RecurrenceId, List<ResourceSkyline>> skylineRecords)
      throws DataFieldNotFoundException, ParseException;
}
