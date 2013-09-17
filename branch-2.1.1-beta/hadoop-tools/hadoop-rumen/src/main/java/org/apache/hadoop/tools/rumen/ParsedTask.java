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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.jobhistory.JhCounters;

/**
 * This is a wrapper class around {@link LoggedTask}. This provides also the
 * extra information about the task obtained from job history which is not
 * written to the JSON trace file.
 */
public class ParsedTask extends LoggedTask {

  private static final Log LOG = LogFactory.getLog(ParsedTask.class);

  private String diagnosticInfo;
  private String failedDueToAttempt;
  private Map<String, Long> countersMap = new HashMap<String, Long>();

  ParsedTask() {
    super();
  }

  public void incorporateCounters(JhCounters counters) {
    Map<String, Long> countersMap =
        JobHistoryUtils.extractCounters(counters);
    putCounters(countersMap);

    super.incorporateCounters(counters);
  }

  /** Set the task counters */
  public void putCounters(Map<String, Long> counters) {
    this.countersMap = counters;
  }

  /**
   * @return the task counters
   */
  public Map<String, Long> obtainCounters() {
    return countersMap;
  }

  /** Set the task diagnostic-info */
  public void putDiagnosticInfo(String msg) {
    diagnosticInfo = msg;
  }

  /**
   * @return the diagnostic-info of this task.
   *         If the task is successful, returns null.
   */
  public String obtainDiagnosticInfo() {
    return diagnosticInfo;
  }

  /**
   * Set the failed-due-to-attemptId info of this task.
   */
  public void putFailedDueToAttemptId(String attempt) {
    failedDueToAttempt = attempt;
  }

  /**
   * @return the failed-due-to-attemptId info of this task.
   *         If the task is successful, returns null.
   */
  public String obtainFailedDueToAttemptId() {
    return failedDueToAttempt;
  }

  /**
   * @return the list of attempts of this task.
   */
  public List<ParsedTaskAttempt> obtainTaskAttempts() {
    List<LoggedTaskAttempt> attempts = getAttempts();
    return convertTaskAttempts(attempts);
  }

  List<ParsedTaskAttempt> convertTaskAttempts(
      List<LoggedTaskAttempt> attempts) {
    List<ParsedTaskAttempt> result = new ArrayList<ParsedTaskAttempt>();

    for (LoggedTaskAttempt t : attempts) {
      if (t instanceof ParsedTaskAttempt) {
        result.add((ParsedTaskAttempt)t);
      } else {
        throw new RuntimeException(
            "Unexpected type of taskAttempts in the list...");
      }
    }
    return result;
  }

  /** Dump the extra info of ParsedTask */
  void dumpParsedTask() {
    LOG.info("ParsedTask details:" + obtainCounters()
        + "\n" + obtainFailedDueToAttemptId()
        + "\nPreferred Locations are:");
    List<LoggedLocation> loc = getPreferredLocations();
    for (LoggedLocation l : loc) {
      LOG.info(l.getLayers() + ";" + l.toString());
    }
    List<ParsedTaskAttempt> attempts = obtainTaskAttempts();
    for (ParsedTaskAttempt attempt : attempts) {
      attempt.dumpParsedTaskAttempt();
    }
  }
}
