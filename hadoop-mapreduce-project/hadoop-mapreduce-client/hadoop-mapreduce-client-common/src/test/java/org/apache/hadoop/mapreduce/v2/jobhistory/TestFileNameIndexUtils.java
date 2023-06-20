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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

import org.junit.jupiter.api.Test;

public class TestFileNameIndexUtils {

  private static final String OLD_JOB_HISTORY_FILE_FORMATTER = "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION;

  private static final String OLD_FORMAT_BEFORE_ADD_START_TIME = "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION;

  private static final String JOB_HISTORY_FILE_FORMATTER = "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + FileNameIndexUtils.DELIMITER + "%s"
      + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION;

  private static final String JOB_ID = "job_1317928501754_0001";
  private static final String SUBMIT_TIME = "1317928742025";
  private static final String USER_NAME = "username";
  private static final String USER_NAME_WITH_DELIMITER = "user"
      + FileNameIndexUtils.DELIMITER + "name";
  private static final String USER_NAME_WITH_DELIMITER_ESCAPE = "user"
      + FileNameIndexUtils.DELIMITER_ESCAPE + "name";
  private static final String JOB_NAME = "mapreduce";
  private static final String JOB_NAME_WITH_DELIMITER = "map"
      + FileNameIndexUtils.DELIMITER + "reduce";
  private static final String JOB_NAME_WITH_DELIMITER_ESCAPE = "map"
      + FileNameIndexUtils.DELIMITER_ESCAPE + "reduce";
  private static final String FINISH_TIME = "1317928754958";
  private static final String NUM_MAPS = "1";
  private static final String NUM_REDUCES = "1";
  private static final String JOB_STATUS = "SUCCEEDED";
  private static final String QUEUE_NAME = "default";
  private static final String QUEUE_NAME_WITH_DELIMITER = "test"
      + FileNameIndexUtils.DELIMITER + "queue";
  private static final String QUEUE_NAME_WITH_DELIMITER_ESCAPE = "test"
      + FileNameIndexUtils.DELIMITER_ESCAPE + "queue";
  private static final String JOB_START_TIME = "1317928742060";

  @Test
  public void testEncodingDecodingEquivalence() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    JobIndexInfo parsedInfo = FileNameIndexUtils.getIndexInfo(jobHistoryFile);

    assertEquals(info.getJobId(), parsedInfo.getJobId(),
        "Job id different after encoding and decoding");
    assertEquals(info.getSubmitTime(), parsedInfo.getSubmitTime(),
        "Submit time different after encoding and decoding");
    assertEquals(info.getUser(), parsedInfo.getUser(),
        "User different after encoding and decoding");
    assertEquals(info.getJobName(), parsedInfo.getJobName(),
        "Job name different after encoding and decoding");
    assertEquals(info.getFinishTime(), parsedInfo.getFinishTime(),
        "Finish time different after encoding and decoding");
    assertEquals(info.getNumMaps(), parsedInfo.getNumMaps(),
        "Num maps different after encoding and decoding");
    assertEquals(info.getNumReduces(), parsedInfo.getNumReduces(),
        "Num reduces different after encoding and decoding");
    assertEquals(info.getJobStatus(), parsedInfo.getJobStatus(),
        "Job status different after encoding and decoding");
    assertEquals(info.getQueueName(), parsedInfo.getQueueName(),
        "Queue name different after encoding and decoding");
    assertEquals(info.getJobStartTime(), parsedInfo.getJobStartTime(),
        "Job start time different after encoding and decoding");
  }

  @Test
  public void testUserNamePercentEncoding() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME_WITH_DELIMITER);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    assertTrue(jobHistoryFile.contains(USER_NAME_WITH_DELIMITER_ESCAPE),
        "User name not encoded correctly into job history file");
  }

  @Test
  public void testTrimJobName() throws IOException {
    int jobNameTrimLength = 5;
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, jobNameTrimLength);
    JobIndexInfo parsedInfo = FileNameIndexUtils.getIndexInfo(jobHistoryFile);

    assertEquals(info.getJobName().substring(0, jobNameTrimLength),
        parsedInfo.getJobName(),
        "Job name did not get trimmed correctly");
  }

  /**
   * Verify the name of jobhistory file is not greater than 255 bytes
   * even if there are some multibyte characters in the job name.
   */
  @Test
  public void testJobNameWithMultibyteChars() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);

    StringBuilder sb = new StringBuilder();
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    // Test for 1 byte UTF-8 character
    // which is encoded into 1 x 3 = 3 characters by URL encode.
    for (int i = 0; i < 100; i++) {
      sb.append('%');
    }
    String longJobName = sb.toString();
    info.setJobName(longJobName);

    String jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, 50);

    assertTrue(jobHistoryFile.length() <= 255);
    String trimedJobName = jobHistoryFile.split(
        FileNameIndexUtils.DELIMITER)[3]; // 3 is index of job name

    // 3 x 16 < 50 < 3 x 17 so the length of trimedJobName should be 48
    assertEquals(48, trimedJobName.getBytes(UTF_8).length);

    // validate whether trimmedJobName by testing reversibility
    byte[] trimedJobNameInByte = trimedJobName.getBytes(UTF_8);
    String reEncodedTrimedJobName = new String(trimedJobNameInByte, UTF_8);
    assertArrayEquals(trimedJobNameInByte,
        reEncodedTrimedJobName.getBytes(UTF_8));
    sb.setLength(0);

    // Test for 2 bytes UTF-8 character
    // which is encoded into 2 x 3 = 6 characters by URL encode.
    for (int i = 0; i < 100; i++) {
      sb.append('\u03A9'); // large omega
    }
    longJobName = sb.toString();
    info.setJobName(longJobName);

    jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, 27);

    assertTrue(jobHistoryFile.length() <= 255);
    trimedJobName = jobHistoryFile.split(
        FileNameIndexUtils.DELIMITER)[3]; // 3 is index of job name

    // 6 x 4 < 27 < 6 x 5 so the length of trimedJobName should be 24
    assertEquals(24, trimedJobName.getBytes(UTF_8).length);

    // validate whether trimmedJobName by testing reversibility
    trimedJobNameInByte = trimedJobName.getBytes(UTF_8);
    reEncodedTrimedJobName = new String(trimedJobNameInByte, UTF_8);
    assertArrayEquals(trimedJobNameInByte,
        reEncodedTrimedJobName.getBytes(UTF_8));
    sb.setLength(0);

    // Test for 3 bytes UTF-8 character
    // which is encoded into 3 x 3 = 9 characters by URL encode.
    for (int i = 0; i < 100; i++) {
      sb.append('\u2192'); // rightwards arrow
    }
    longJobName = sb.toString();
    info.setJobName(longJobName);

    jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, 40);

    assertTrue(jobHistoryFile.length() <= 255);
    trimedJobName = jobHistoryFile.split(
        FileNameIndexUtils.DELIMITER)[3]; // 3 is index of job name

    // 9 x 4 < 40 < 9 x 5 so the length of trimedJobName should be 36
    assertEquals(36, trimedJobName.getBytes(UTF_8).length);

    // validate whether trimmedJobName by testing reversibility
    trimedJobNameInByte = trimedJobName.getBytes(UTF_8);
    reEncodedTrimedJobName = new String(trimedJobNameInByte, UTF_8);
    assertArrayEquals(trimedJobNameInByte,
        reEncodedTrimedJobName.getBytes(UTF_8));
    sb.setLength(0);

    // Test for 4 bytes UTF-8 character
    // which is encoded into 4 x 3 = 12 characters by URL encode.
    for (int i = 0; i < 100; i++) {
      sb.append("\uD867\uDE3D"); // Mugil cephalus in Kanji.
    }
    longJobName = sb.toString();
    info.setJobName(longJobName);

    jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, 49);

    assertTrue(jobHistoryFile.length() <= 255);
    trimedJobName = jobHistoryFile.split(
        FileNameIndexUtils.DELIMITER)[3]; // 3 is index of job name

    // 12 x 4 < 49 < 12 x 5 so the length of trimedJobName should be 48
    assertEquals(48, trimedJobName.getBytes(UTF_8).length);

    // validate whether trimmedJobName by testing reversibility
    trimedJobNameInByte = trimedJobName.getBytes(UTF_8);
    reEncodedTrimedJobName = new String(trimedJobNameInByte, UTF_8);
    assertArrayEquals(trimedJobNameInByte,
        reEncodedTrimedJobName.getBytes(UTF_8));
    sb.setLength(0);

    // Test for the combination of 1 to 4 bytes UTF-8 characters
    sb.append('\u732B') // cat in Kanji (encoded into 3 bytes x 3 characters)
        .append("[") // (encoded into 1 byte x 3 characters)
        .append('\u03BB') // small lambda (encoded into 2 bytes x 3 characters)
        .append('/') // (encoded into 1 byte x 3 characters)
        .append('A') // not url-encoded (1 byte x 1 character)
        .append("\ud867\ude49") // flying fish in
        // Kanji (encoded into 4 bytes x 3 characters)
        .append('\u72AC'); // dog in Kanji (encoded into 3 bytes x 3 characters)

    longJobName = sb.toString();
    info.setJobName(longJobName);

    jobHistoryFile =
        FileNameIndexUtils.getDoneFileName(info, 23);

    assertTrue(jobHistoryFile.length() <= 255);
    trimedJobName = jobHistoryFile.split(
        FileNameIndexUtils.DELIMITER)[3]; // 3 is index of job name

    // total size of the first 5 characters = 22
    // 23 < total size of the first 6 characters
    assertEquals(22, trimedJobName.getBytes(UTF_8).length);

    // validate whether trimmedJobName by testing reversibility
    trimedJobNameInByte = trimedJobName.getBytes(UTF_8);
    reEncodedTrimedJobName = new String(trimedJobNameInByte, UTF_8);
    assertArrayEquals(trimedJobNameInByte,
        reEncodedTrimedJobName.getBytes(UTF_8));
  }

  @Test
  public void testUserNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME_WITH_DELIMITER_ESCAPE,
        JOB_NAME,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS,
        QUEUE_NAME,
        JOB_START_TIME);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    assertEquals(USER_NAME_WITH_DELIMITER, info.getUser(), "User name doesn't match");
  }

  @Test
  public void testJobNamePercentEncoding() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME_WITH_DELIMITER);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    assertTrue(jobHistoryFile.contains(JOB_NAME_WITH_DELIMITER_ESCAPE),
        "Job name not encoded correctly into job history file");
  }

  @Test
  public void testJobNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME,
        JOB_NAME_WITH_DELIMITER_ESCAPE,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS,
        QUEUE_NAME,
        JOB_START_TIME);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    assertEquals(JOB_NAME_WITH_DELIMITER, info.getJobName(), "Job name doesn't match");
  }

  @Test
  public void testQueueNamePercentEncoding() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME_WITH_DELIMITER);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    assertTrue(jobHistoryFile.contains(QUEUE_NAME_WITH_DELIMITER_ESCAPE),
        "Queue name not encoded correctly into job history file");
  }

  @Test
  public void testQueueNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME,
        JOB_NAME,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS,
        QUEUE_NAME_WITH_DELIMITER_ESCAPE,
        JOB_START_TIME);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    assertEquals(QUEUE_NAME_WITH_DELIMITER, info.getQueueName(), "Queue name doesn't match");
  }

  @Test
  public void testJobStartTimeBackwardsCompatible() throws IOException {
    String jobHistoryFile = String.format(OLD_FORMAT_BEFORE_ADD_START_TIME,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME,
        JOB_NAME_WITH_DELIMITER_ESCAPE,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS,
        QUEUE_NAME);
    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    assertEquals(info.getJobStartTime(), info.getSubmitTime());
  }

  @Test
  public void testJobHistoryFileNameBackwardsCompatible() throws IOException {
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);

    long submitTime = Long.parseLong(SUBMIT_TIME);
    long finishTime = Long.parseLong(FINISH_TIME);
    int numMaps = Integer.parseInt(NUM_MAPS);
    int numReduces = Integer.parseInt(NUM_REDUCES);

    String jobHistoryFile = String.format(OLD_JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME,
        JOB_NAME,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    assertEquals(jobId, info.getJobId(), "Job id incorrect after decoding old history file");
    assertEquals(submitTime, info.getSubmitTime(),
        "Submit time incorrect after decoding old history file");
    assertEquals(USER_NAME, info.getUser(), "User incorrect after decoding old history file");
    assertEquals(JOB_NAME, info.getJobName(), "Job name incorrect after decoding old history file");
    assertEquals(finishTime, info.getFinishTime(),
        "Finish time incorrect after decoding old history file");
    assertEquals(numMaps, info.getNumMaps(), "Num maps incorrect after decoding old history file");
    assertEquals(numReduces, info.getNumReduces(),
        "Num reduces incorrect after decoding old history file");
    assertEquals(JOB_STATUS, info.getJobStatus(),
        "Job status incorrect after decoding old history file");
    assertNull(info.getQueueName(), "Queue name incorrect after decoding old history file");
  }

  @Test
  public void testTrimJobNameEqualsLimitLength() throws IOException {
    int jobNameTrimLength = 9;
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info,
        jobNameTrimLength);
    JobIndexInfo parsedInfo = FileNameIndexUtils.getIndexInfo(jobHistoryFile);

    assertEquals(info.getJobName().substring(0, jobNameTrimLength), parsedInfo.getJobName(),
        "Job name did not get trimmed correctly");
  }
}
