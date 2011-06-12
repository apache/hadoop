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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;

/**
 * Job History related utils for handling multiple formats of history logs of
 * different hadoop versions like Pre21 history logs, current history logs.
 */
public class JobHistoryUtils {

  private static String applyParser(String fileName, Pattern pattern) {
    Matcher matcher = pattern.matcher(fileName);

    if (!matcher.matches()) {
      return null;
    }

    return matcher.group(1);
  }

  /**
   * Extracts jobID string from the given job history log file name or
   * job history configuration file name.
   * @param fileName name of job history file or job history configuration file
   * @return a valid jobID String, parsed out of the file name. Otherwise,
   *         [especially for .crc files] returns null.
   */
  static String extractJobID(String fileName) {
    // Get jobID if fileName is a config file name.
    String jobId = extractJobIDFromConfFileName(fileName);
    if (jobId == null) {
      // Get JobID if fileName is a job history file name
      jobId = extractJobIDFromHistoryFileName(fileName);
    }
    return jobId;
  }

  /**
   * Extracts jobID string from the given job history file name.
   * @param fileName name of the job history file
   * @return JobID if the given <code>fileName</code> is a valid job history
   *         file name, <code>null</code> otherwise.
   */
  private static String extractJobIDFromHistoryFileName(String fileName) {
    // History file name could be in one of the following formats
    // (1) old pre21 job history file name format
    // (2) new pre21 job history file name format
    // (3) current job history file name format i.e. 0.22
    String pre21JobID = applyParser(fileName,
        Pre21JobHistoryConstants.JOBHISTORY_FILENAME_REGEX_V1);
    if (pre21JobID == null) {
      pre21JobID = applyParser(fileName,
          Pre21JobHistoryConstants.JOBHISTORY_FILENAME_REGEX_V2);
    }
    if (pre21JobID != null) {
      return pre21JobID;
    }
    return applyParser(fileName, JobHistory.JOBHISTORY_FILENAME_REGEX);
  }

  /**
   * Extracts jobID string from the given job conf xml file name.
   * @param fileName name of the job conf xml file
   * @return job id if the given <code>fileName</code> is a valid job conf xml
   *         file name, <code>null</code> otherwise.
   */
  private static String extractJobIDFromConfFileName(String fileName) {
    // History conf file name could be in one of the following formats
    // (1) old pre21 job history file name format
    // (2) new pre21 job history file name format
    // (3) current job history file name format i.e. 0.22
    String pre21JobID = applyParser(fileName,
                          Pre21JobHistoryConstants.CONF_FILENAME_REGEX_V1);
    if (pre21JobID == null) {
      pre21JobID = applyParser(fileName,
                     Pre21JobHistoryConstants.CONF_FILENAME_REGEX_V2);
    }
    if (pre21JobID != null) {
      return pre21JobID;
    }
    return applyParser(fileName, JobHistory.CONF_FILENAME_REGEX);
  }

  /**
   * Checks if the given <code>fileName</code> is a valid job conf xml file name
   * @param fileName name of the file to be validated
   * @return <code>true</code> if the given <code>fileName</code> is a valid
   *         job conf xml file name.
   */
  static boolean isJobConfXml(String fileName) {
    String jobId = extractJobIDFromConfFileName(fileName);
    return jobId != null;
  }
}
