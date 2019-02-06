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

package org.apache.hadoop.tools.rumen;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapreduce.jobhistory.JhCounters;

/**
 * This is a wrapper class around {@link LoggedTaskAttempt}. This provides
 * also the extra information about the task attempt obtained from
 * job history which is not written to the JSON trace file.
 */
public class ParsedTaskAttempt extends LoggedTaskAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(ParsedTaskAttempt.class);

  private String diagnosticInfo;
  private String trackerName;
  private Integer httpPort, shufflePort;
  private Map<String, Long> countersMap = new HashMap<String, Long>();

  ParsedTaskAttempt() {
    super();
  }

  /** incorporate event counters */
  public void incorporateCounters(JhCounters counters) {

    Map<String, Long> countersMap =
      JobHistoryUtils.extractCounters(counters);
    putCounters(countersMap);

    super.incorporateCounters(counters);
  }

  /** Set the task attempt counters */
  public void putCounters(Map<String, Long> counters) {
    this.countersMap = counters;
  }

  /**
   * @return the task attempt counters
   */
  public Map<String, Long> obtainCounters() {
    return countersMap;
  }

  /** Set the task attempt diagnostic-info */
  public void putDiagnosticInfo(String msg) {
    diagnosticInfo = msg;
  }

  /**
   * @return the diagnostic-info of this task attempt.
   *         If the attempt is successful, returns null.
   */
  public String obtainDiagnosticInfo() {
    return diagnosticInfo;
  }

  void putTrackerName(String trackerName) {
    this.trackerName = trackerName;
  }

  public String obtainTrackerName() {
    return trackerName;
  }

  void putHttpPort(int port) {
    httpPort = port;
  }

  /**
   * @return http port if set. Returns null otherwise.
   */
  public Integer obtainHttpPort() {
    return httpPort;
  }

  void putShufflePort(int port) {
    shufflePort = port;
  }

  /**
   * @return shuffle port if set. Returns null otherwise.
   */
  public Integer obtainShufflePort() {
    return shufflePort;
  }

  /** Dump the extra info of ParsedTaskAttempt */
  void dumpParsedTaskAttempt() {
    LOG.info("ParsedTaskAttempt details:" + obtainCounters()
        + ";DiagnosticInfo=" + obtainDiagnosticInfo() + "\n"
        + obtainTrackerName() + ";" + obtainHttpPort() + ";"
        + obtainShufflePort() + ";rack=" + getHostName().getRackName()
        + ";host=" + getHostName().getHostName());
  }
}
