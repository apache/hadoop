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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class FileNameIndexUtils {

  // Sanitize job history file for predictable parsing
  static final String DELIMITER = "-";
  static final String DELIMITER_ESCAPE = "%2D";

  private static final Log LOG = LogFactory.getLog(FileNameIndexUtils.class);

  // Job history file names need to be backwards compatible
  // Only append new elements to the end of this list
  private static final int JOB_ID_INDEX = 0;
  private static final int SUBMIT_TIME_INDEX = 1;
  private static final int USER_INDEX = 2;
  private static final int JOB_NAME_INDEX = 3;
  private static final int FINISH_TIME_INDEX = 4;
  private static final int NUM_MAPS_INDEX = 5;
  private static final int NUM_REDUCES_INDEX = 6;
  private static final int JOB_STATUS_INDEX = 7;
  private static final int QUEUE_NAME_INDEX = 8;
  private static final int JOB_START_TIME_INDEX = 9;

  /**
   * Constructs the job history file name from the JobIndexInfo.
   * 
   * @param indexInfo the index info.
   * @return the done job history filename.
   */
  public static String getDoneFileName(JobIndexInfo indexInfo)
      throws IOException {
    return getDoneFileName(indexInfo,
        JHAdminConfig.DEFAULT_MR_HS_JOBNAME_LIMIT);
  }

  public static String getDoneFileName(JobIndexInfo indexInfo,
      int jobNameLimit) throws IOException {
    StringBuilder sb = new StringBuilder();
    //JobId
    sb.append(encodeJobHistoryFileName(escapeDelimiters(
        TypeConverter.fromYarn(indexInfo.getJobId()).toString())));
    sb.append(DELIMITER);

    //SubmitTime
    sb.append(encodeJobHistoryFileName(String.valueOf(
        indexInfo.getSubmitTime())));
    sb.append(DELIMITER);

    //UserName
    sb.append(encodeJobHistoryFileName(escapeDelimiters(
        getUserName(indexInfo))));
    sb.append(DELIMITER);

    //JobName
    sb.append(trimURLEncodedString(encodeJobHistoryFileName(escapeDelimiters(
        getJobName(indexInfo))), jobNameLimit));
    sb.append(DELIMITER);

    //FinishTime
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getFinishTime())));
    sb.append(DELIMITER);

    //NumMaps
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getNumMaps())));
    sb.append(DELIMITER);

    //NumReduces
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getNumReduces())));
    sb.append(DELIMITER);

    //JobStatus
    sb.append(encodeJobHistoryFileName(indexInfo.getJobStatus()));
    sb.append(DELIMITER);

    //QueueName
    sb.append(escapeDelimiters(encodeJobHistoryFileName(
        getQueueName(indexInfo))));
    sb.append(DELIMITER);

    //JobStartTime
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getJobStartTime())));

    sb.append(encodeJobHistoryFileName(
        JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
    return sb.toString();
  }

  /**
   * Parses the provided job history file name to construct a
   * JobIndexInfo object which is returned.
   * 
   * @param jhFileName the job history filename.
   * @return a JobIndexInfo object built from the filename.
   */
  public static JobIndexInfo getIndexInfo(String jhFileName)
      throws IOException {
    String fileName = jhFileName.substring(0,
        jhFileName.indexOf(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
    JobIndexInfo indexInfo = new JobIndexInfo();

    String[] jobDetails = fileName.split(DELIMITER);

    JobID oldJobId =
        JobID.forName(decodeJobHistoryFileName(jobDetails[JOB_ID_INDEX]));
    JobId jobId = TypeConverter.toYarn(oldJobId);
    indexInfo.setJobId(jobId);

    // Do not fail if there are some minor parse errors
    try {
      try {
        indexInfo.setSubmitTime(Long.parseLong(
            decodeJobHistoryFileName(jobDetails[SUBMIT_TIME_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse submit time from job history file "
            + jhFileName + " : " + e);
      }

      indexInfo.setUser(
          decodeJobHistoryFileName(jobDetails[USER_INDEX]));

      indexInfo.setJobName(
          decodeJobHistoryFileName(jobDetails[JOB_NAME_INDEX]));

      try {
        indexInfo.setFinishTime(Long.parseLong(
            decodeJobHistoryFileName(jobDetails[FINISH_TIME_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse finish time from job history file "
            + jhFileName + " : " + e);
      }

      try {
        indexInfo.setNumMaps(Integer.parseInt(
            decodeJobHistoryFileName(jobDetails[NUM_MAPS_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse num maps from job history file "
            + jhFileName + " : " + e);
      }

      try {
        indexInfo.setNumReduces(Integer.parseInt(
            decodeJobHistoryFileName(jobDetails[NUM_REDUCES_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse num reduces from job history file "
            + jhFileName + " : " + e);
      }

      indexInfo.setJobStatus(
          decodeJobHistoryFileName(jobDetails[JOB_STATUS_INDEX]));

      indexInfo.setQueueName(
          decodeJobHistoryFileName(jobDetails[QUEUE_NAME_INDEX]));

      try{
        if (jobDetails.length <= JOB_START_TIME_INDEX) {
          indexInfo.setJobStartTime(indexInfo.getSubmitTime());
        } else {
          indexInfo.setJobStartTime(Long.parseLong(
              decodeJobHistoryFileName(jobDetails[JOB_START_TIME_INDEX])));
        }
      } catch (NumberFormatException e){
        LOG.warn("Unable to parse start time from job history file "
            + jhFileName + " : " + e);
      }
    } catch (IndexOutOfBoundsException e) {
      LOG.warn("Parsing job history file with partial data encoded into name: "
          + jhFileName);
    }

    return indexInfo;
  }

  
  /**
   * Helper function to encode the URL of the filename of the job-history
   * log file.
   * 
   * @param logFileName file name of the job-history file
   * @return URL encoded filename
   * @throws IOException
   */
  public static String encodeJobHistoryFileName(String logFileName)
  throws IOException {
    String replacementDelimiterEscape = null;

    // Temporarily protect the escape delimiters from encoding
    if (logFileName.contains(DELIMITER_ESCAPE)) {
      replacementDelimiterEscape = nonOccursString(logFileName);

      logFileName = logFileName.replaceAll(
          DELIMITER_ESCAPE, replacementDelimiterEscape);
    }

    String encodedFileName = null;
    try {
      encodedFileName = URLEncoder.encode(logFileName, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      IOException ioe = new IOException();
      ioe.initCause(uee);
      ioe.setStackTrace(uee.getStackTrace());
      throw ioe;
    }

    // Restore protected escape delimiters after encoding
    if (replacementDelimiterEscape != null) {
      encodedFileName = encodedFileName.replaceAll(
          replacementDelimiterEscape, DELIMITER_ESCAPE);
    }

    return encodedFileName;
  }

  /**
   * Helper function to decode the URL of the filename of the job-history
   * log file.
   * 
   * @param logFileName file name of the job-history file
   * @return URL decoded filename
   * @throws IOException
   */
  public static String decodeJobHistoryFileName(String logFileName)
  throws IOException {
    String decodedFileName = null;
    try {
      decodedFileName = URLDecoder.decode(logFileName, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      IOException ioe = new IOException();
      ioe.initCause(uee);
      ioe.setStackTrace(uee.getStackTrace());
      throw ioe;
    }
    return decodedFileName;
  }

  static String nonOccursString(String logFileName) {
    int adHocIndex = 0;

    String unfoundString = "q" + adHocIndex;

    while (logFileName.contains(unfoundString)) {
      unfoundString = "q" + ++adHocIndex;
    }

    return unfoundString + "q";
  }

  private static String getUserName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getUser());
  }

  private static String getJobName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getJobName());
  }

  private static String getQueueName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getQueueName());
  }

  //TODO Maybe handle default values for longs and integers here?
  
  private static String getNonEmptyString(String in) {
    if (in == null || in.length() == 0) {
      in = "NA";
    }
    return in;
  }

  private static String escapeDelimiters(String escapee) {
    return escapee.replaceAll(DELIMITER, DELIMITER_ESCAPE);
  }

  /**
   * Trims the url-encoded string if required
   */
  private static String trimURLEncodedString(
      String encodedString, int limitLength) {
    assert(limitLength >= 0) : "limitLength should be positive integer";

    if (encodedString.length() <= limitLength) {
      return encodedString;
    }

    int index = 0;
    int increase = 0;
    byte[] strBytes = encodedString.getBytes(UTF_8);

    // calculate effective character length based on UTF-8 specification.
    // The size of a character coded in UTF-8 should be 4-byte at most.
    // See RFC3629
    while (true) {
      byte b = strBytes[index];
      if (b == '%') {
        byte minuend1 = strBytes[index + 1];
        byte subtrahend1 = (byte)(Character.isDigit(
            minuend1) ? '0' : 'A' - 10);
        byte minuend2 = strBytes[index + 2];
        byte subtrahend2 = (byte)(Character.isDigit(
            minuend2) ? '0' : 'A' - 10);
        int initialHex =
            ((Character.toUpperCase(minuend1) - subtrahend1) << 4) +
            (Character.toUpperCase(minuend2) - subtrahend2);

        if (0x00 <= initialHex && initialHex <= 0x7F) {
          // For 1-byte UTF-8 characters
          increase = 3;
        } else if (0xC2 <= initialHex && initialHex <= 0xDF) {
          // For 2-byte UTF-8 characters
          increase = 6;
        } else if (0xE0 <= initialHex && initialHex <= 0xEF) {
          // For 3-byte UTF-8 characters
          increase = 9;
        } else {
          // For 4-byte UTF-8 characters
          increase = 12;
        }
      } else {
        increase = 1;
      }
      if (index + increase > limitLength) {
        break;
      } else {
        index += increase;
      }
    }

    return encodedString.substring(0, index);
  }
}
