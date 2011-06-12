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

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class FileNameIndexUtils {

  static final String UNDERSCORE_ESCAPE = "%5F";
  static final int JOB_NAME_TRIM_LENGTH = 50;
  
  //This has to be underscore currently. Untill escape uses DELIMITER.
  static final String DELIMITER = "_";
  
  private static final int JOB_ID_INDEX = 0;
  private static final int SUBMIT_TIME_INDEX = 1;
  private static final int USER_INDEX = 2;
  private static final int JOB_NAME_INDEX = 3;
  private static final int FINISH_TIME_INDEX = 4;
  private static final int NUM_MAPS_INDEX = 5;
  private static final int NUM_REDUCES_INDEX = 6;
  private static final int MAX_INDEX = NUM_REDUCES_INDEX;

  /**
   * Constructs the job history file name from the JobIndexInfo.
   * 
   * @param indexInfo the index info.
   * @return the done job history filename.
   */
  public static String getDoneFileName(JobIndexInfo indexInfo) throws IOException {
    StringBuilder sb = new StringBuilder();
    //JobId
    sb.append(escapeUnderscores(TypeConverter.fromYarn(indexInfo.getJobId()).toString()));
    sb.append(DELIMITER);
    
    //StartTime
    sb.append(indexInfo.getSubmitTime());
    sb.append(DELIMITER);
    
    //UserName
    sb.append(escapeUnderscores(getUserName(indexInfo)));
    sb.append(DELIMITER);
    
    //JobName
    sb.append(escapeUnderscores(trimJobName(getJobName(indexInfo))));
    sb.append(DELIMITER);
    
    //FinishTime
    sb.append(indexInfo.getFinishTime());
    sb.append(DELIMITER);
    
    //NumMaps
    sb.append(indexInfo.getNumMaps());
    sb.append(DELIMITER);
    
    //NumReduces
    sb.append(indexInfo.getNumReduces());
    
    sb.append(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION);
    return encodeJobHistoryFileName(sb.toString());
  }
  
  /**
   * Parses the provided job history file name to construct a
   * JobIndexInfo object which is returned.
   * 
   * @param jhFileName the job history filename.
   * @return a JobIndexInfo object built from the filename.
   */
  public static JobIndexInfo getIndexInfo(String jhFileName) throws IOException {
    String fileName = jhFileName.substring(0, jhFileName.indexOf(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
    JobIndexInfo indexInfo = new JobIndexInfo();
    
    String[] jobDetails = fileName.split(DELIMITER);
    if (jobDetails.length != MAX_INDEX +1) {
      throw new IOException("Failed to parse file: [" + jhFileName + "]. Expected " + (MAX_INDEX + 1) + "parts.");  
    }
    
    JobID oldJobId = JobID.forName(decodeJobHistoryFileName(jobDetails[JOB_ID_INDEX]));
    JobId jobId = TypeConverter.toYarn(oldJobId);
    indexInfo.setJobId(jobId);
    //TODO Catch NumberFormatException - Do not fail if there's only a few fields missing.
    indexInfo.setSubmitTime(Long.parseLong(decodeJobHistoryFileName(jobDetails[SUBMIT_TIME_INDEX])));
    
    indexInfo.setUser(decodeJobHistoryFileName(jobDetails[USER_INDEX]));
    
    indexInfo.setJobName(decodeJobHistoryFileName(jobDetails[JOB_NAME_INDEX]));
    
    indexInfo.setFinishTime(Long.parseLong(decodeJobHistoryFileName(jobDetails[FINISH_TIME_INDEX])));
    
    indexInfo.setNumMaps(Integer.parseInt(decodeJobHistoryFileName(jobDetails[NUM_MAPS_INDEX])));
    
    indexInfo.setNumReduces(Integer.parseInt(decodeJobHistoryFileName(jobDetails[NUM_REDUCES_INDEX])));
    
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
    String replacementUnderscoreEscape = null;

    if (logFileName.contains(UNDERSCORE_ESCAPE)) {
      replacementUnderscoreEscape = nonOccursString(logFileName);

      logFileName = replaceStringInstances
        (logFileName, UNDERSCORE_ESCAPE, replacementUnderscoreEscape);
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
    
    if (replacementUnderscoreEscape != null) {
      encodedFileName = replaceStringInstances
        (encodedFileName, replacementUnderscoreEscape, UNDERSCORE_ESCAPE);
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

  //TODO Maybe handle default values for longs and integers here?
  
  private static String getNonEmptyString(String in) {
    if (in == null || in.length() == 0) {
      in = "NA";
    }
    return in;
  }
  
  private static String escapeUnderscores(String escapee) {
    return replaceStringInstances(escapee, "_", UNDERSCORE_ESCAPE);
  }
  
  // I tolerate this code because I expect a low number of
  // occurrences in a relatively short string
  private static String replaceStringInstances
      (String logFileName, String old, String replacement) {
    int index = logFileName.indexOf(old);

    while (index > 0) {
      logFileName = (logFileName.substring(0, index)
                     + replacement
                     + replaceStringInstances
                         (logFileName.substring(index + old.length()),
                          old, replacement));

      index = logFileName.indexOf(old);
    }

    return logFileName;
  }
  
  /**
   * Trims the job-name if required
   */
  private static String trimJobName(String jobName) {
    if (jobName.length() > JOB_NAME_TRIM_LENGTH) {
      jobName = jobName.substring(0, JOB_NAME_TRIM_LENGTH);
    }
    return jobName;
  }
}
