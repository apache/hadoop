/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * Merge all the audit logs present in a directory of
 * multiple audit log files into a single audit log file.
 */
public class S3AAuditLogMergerAndParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AAuditLogMergerAndParser.class);

  /**
   * Simple entry: anything up to a space.
   * {@value}.
   */
  private static final String SIMPLE = "[^ ]*";

  /**
   * Date/Time. Everything within square braces.
   * {@value}.
   */
  private static final String DATETIME = "\\[(.*?)\\]";

  /**
   * A natural number or "-".
   * {@value}.
   */
  private static final String NUMBER = "(-|[0-9]*)";

  /**
   * A Quoted field or "-".
   * {@value}.
   */
  private static final String QUOTED = "(-|\"[^\"]*\")";

  /**
   * An entry in the regexp.
   *
   * @param name    name of the group
   * @param pattern pattern to use in the regexp
   * @return the pattern for the regexp
   */
  private static String e(String name, String pattern) {
    return String.format("(?<%s>%s) ", name, pattern);
  }

  /**
   * An entry in the regexp.
   *
   * @param name    name of the group
   * @param pattern pattern to use in the regexp
   * @return the pattern for the regexp
   */
  private static String eNoTrailing(String name, String pattern) {
    return String.format("(?<%s>%s)", name, pattern);
  }

  /**
   * Simple entry using the {@link #SIMPLE} pattern.
   *
   * @param name name of the element (for code clarity only)
   * @return the pattern for the regexp
   */
  private static String e(String name) {
    return e(name, SIMPLE);
  }

  /**
   * Quoted entry using the {@link #QUOTED} pattern.
   *
   * @param name name of the element (for code clarity only)
   * @return the pattern for the regexp
   */
  private static String q(String name) {
    return e(name, QUOTED);
  }

  /**
   * Log group {@value}.
   */
  public static final String OWNER_GROUP = "owner";

  /**
   * Log group {@value}.
   */
  public static final String BUCKET_GROUP = "bucket";

  /**
   * Log group {@value}.
   */
  public static final String TIMESTAMP_GROUP = "timestamp";

  /**
   * Log group {@value}.
   */
  public static final String REMOTEIP_GROUP = "remoteip";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTER_GROUP = "requester";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTID_GROUP = "requestid";

  /**
   * Log group {@value}.
   */
  public static final String VERB_GROUP = "verb";

  /**
   * Log group {@value}.
   */
  public static final String KEY_GROUP = "key";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTURI_GROUP = "requesturi";

  /**
   * Log group {@value}.
   */
  public static final String HTTP_GROUP = "http";

  /**
   * Log group {@value}.
   */
  public static final String AWSERRORCODE_GROUP = "awserrorcode";

  /**
   * Log group {@value}.
   */
  public static final String BYTESSENT_GROUP = "bytessent";

  /**
   * Log group {@value}.
   */
  public static final String OBJECTSIZE_GROUP = "objectsize";

  /**
   * Log group {@value}.
   */
  public static final String TOTALTIME_GROUP = "totaltime";

  /**
   * Log group {@value}.
   */
  public static final String TURNAROUNDTIME_GROUP = "turnaroundtime";

  /**
   * Log group {@value}.
   */
  public static final String REFERRER_GROUP = "referrer";

  /**
   * Log group {@value}.
   */
  public static final String USERAGENT_GROUP = "useragent";

  /**
   * Log group {@value}.
   */
  public static final String VERSION_GROUP = "version";

  /**
   * Log group {@value}.
   */
  public static final String HOSTID_GROUP = "hostid";

  /**
   * Log group {@value}.
   */
  public static final String SIGV_GROUP = "sigv";

  /**
   * Log group {@value}.
   */
  public static final String CYPHER_GROUP = "cypher";

  /**
   * Log group {@value}.
   */
  public static final String AUTH_GROUP = "auth";

  /**
   * Log group {@value}.
   */
  public static final String ENDPOINT_GROUP = "endpoint";

  /**
   * Log group {@value}.
   */
  public static final String TLS_GROUP = "tls";

  /**
   * This is where anything at the tail of a log.
   * entry ends up; it is null unless/until the AWS
   * logs are enhanced in future.
   * Value {@value}.
   */
  public static final String TAIL_GROUP = "tail";

  /**
   * Construct the log entry pattern.
   */
  public static final String LOG_ENTRY_REGEXP = ""
      + e(OWNER_GROUP)
      + e(BUCKET_GROUP)
      + e(TIMESTAMP_GROUP, DATETIME)
      + e(REMOTEIP_GROUP)
      + e(REQUESTER_GROUP)
      + e(REQUESTID_GROUP)
      + e(VERB_GROUP)
      + e(KEY_GROUP)
      + q(REQUESTURI_GROUP)
      + e(HTTP_GROUP, NUMBER)
      + e(AWSERRORCODE_GROUP)
      + e(BYTESSENT_GROUP)
      + e(OBJECTSIZE_GROUP)
      + e(TOTALTIME_GROUP)
      + e(TURNAROUNDTIME_GROUP)
      + q(REFERRER_GROUP)
      + q(USERAGENT_GROUP)
      + e(VERSION_GROUP)
      + e(HOSTID_GROUP)
      + e(SIGV_GROUP)
      + e(CYPHER_GROUP)
      + e(AUTH_GROUP)
      + e(ENDPOINT_GROUP)
      + eNoTrailing(TLS_GROUP, SIMPLE)
      + eNoTrailing(TAIL_GROUP, ".*") // anything which follows
      + "$"; // end of line

  /**
   * Groups in order.
   */
  private static final String[] GROUPS = {
      OWNER_GROUP,
      BUCKET_GROUP,
      TIMESTAMP_GROUP,
      REMOTEIP_GROUP,
      REQUESTER_GROUP,
      REQUESTID_GROUP,
      VERB_GROUP,
      KEY_GROUP,
      REQUESTURI_GROUP,
      HTTP_GROUP,
      AWSERRORCODE_GROUP,
      BYTESSENT_GROUP,
      OBJECTSIZE_GROUP,
      TOTALTIME_GROUP,
      TURNAROUNDTIME_GROUP,
      REFERRER_GROUP,
      USERAGENT_GROUP,
      VERSION_GROUP,
      HOSTID_GROUP,
      SIGV_GROUP,
      CYPHER_GROUP,
      AUTH_GROUP,
      ENDPOINT_GROUP,
      TLS_GROUP,
      TAIL_GROUP
  };

  /**
   * Ordered list of regular expression group names.
   */
  public static final List<String> AWS_LOG_REGEXP_GROUPS =
      Collections.unmodifiableList(Arrays.asList(GROUPS));

  /**
   * And the actual compiled pattern.
   */
  public static final Pattern LOG_ENTRY_PATTERN = Pattern.compile(
      LOG_ENTRY_REGEXP);

  /**
   * parseAuditLog method helps in parsing the audit log.
   * into key-value pairs using regular expression
   *
   * @param singleAuditLog this is single audit log from merged audit log file
   * @return it returns a map i.e, auditLogMap which contains key-value pairs of a single audit log
   */
  public Map<String, String> parseAuditLog(String singleAuditLog) {
    Map<String, String> auditLogMap = new HashMap<>();
    if (singleAuditLog == null || singleAuditLog.length() == 0) {
      LOG.info(
          "This is an empty string or null string, expected a valid string to parse");
      return auditLogMap;
    }
    final Matcher matcher = LOG_ENTRY_PATTERN.matcher(singleAuditLog);
    boolean patternMatching = matcher.matches();
    if (patternMatching) {
      for (String key : AWS_LOG_REGEXP_GROUPS) {
        try {
          final String value = matcher.group(key);
          auditLogMap.put(key, value);
        } catch (IllegalStateException e) {
          LOG.info(String.valueOf(e));
        }
      }
    }
    return auditLogMap;
  }

  /**
   * parseReferrerHeader method helps in parsing the http referrer header.
   * which is one of the key-value pair of audit log
   *
   * @param referrerHeader this is the http referrer header of a particular audit log
   * @return it returns a map i.e, auditLogMap which contains key-value pairs
   * of audit log as well as referrer header present in it
   */
  public Map<String, String> parseReferrerHeader(String referrerHeader) {
    Map<String, String> referrerHeaderMap = new HashMap<>();
    if (referrerHeader == null || referrerHeader.length() == 0) {
      LOG.info(
          "This is an empty string or null string, expected a valid string to parse");
      return referrerHeaderMap;
    }
    int indexOfQuestionMark = referrerHeader.indexOf("?");
    String httpReferrer = referrerHeader.substring(indexOfQuestionMark + 1,
        referrerHeader.length() - 1);
    int lengthOfReferrer = httpReferrer.length();
    int start = 0;
    while (start < lengthOfReferrer) {
      int equals = httpReferrer.indexOf("=", start);
      // no match : break
      if (equals == -1) {
        break;
      }
      String key = httpReferrer.substring(start, equals);
      int end = httpReferrer.indexOf("&", equals);
      // or end of string
      if (end == -1) {
        end = lengthOfReferrer;
      }
      String value = httpReferrer.substring(equals + 1, end);
      referrerHeaderMap.put(key, value);
      start = end + 1;
    }
    return referrerHeaderMap;
  }

  /**
   * convertToAvroFile method converts list of maps.
   * into avro file by serializing
   *
   * @param referrerHeaderList list of maps which contains key-value pairs of
   *                           only referrer header
   * @param auditLogList       list of maps which contains key-value pairs
   *                           of audit log except referrer header
   * @throws IOException
   */
  private void convertToAvroFile(
      List<HashMap<String, String>> referrerHeaderList,
      List<HashMap<String, String>> auditLogList, S3AFileSystem s3AFileSystem,
      Path s3DestPath) throws IOException {

    // Instantiating generated AvroDataRecord class
    AvroDataRecord avroDataRecord = new AvroDataRecord();

    // Instantiate DatumWriter class
    DatumWriter<AvroDataRecord> datumWriter =
        new SpecificDatumWriter<AvroDataRecord>(AvroDataRecord.class);
    DataFileWriter<AvroDataRecord> dataFileWriter =
        new DataFileWriter<AvroDataRecord>(datumWriter);

    dataFileWriter.create(avroDataRecord.getSchema(),
        new File("AvroData.avro"));

    ArrayList<String> longValues = new ArrayList<>(
        Arrays.asList("turnaroundtime", "bytessent", "objectsize",
            "totaltime"));
    int count = 0;

    // Write avro data into a file in bucket destination path
    Path avroFile = new Path(s3DestPath, "AvroData.avro");

    try (FSDataOutputStream fsDataOutputStream = s3AFileSystem.create(
        avroFile)) {
      // Insert data according to schema
      for (Map<String, String> auditLogMap : auditLogList) {
        for (Map.Entry<String, String> entry : auditLogMap.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue().trim();

          // if value == '-' and key is not in arraylist then put '-' or else '-1'
          // if key is in arraylist of long values then parse the long value
          // while parsing do it in try-catch block,
          // in catch block need to log exception and set value as '-1'
          try {
            if (longValues.contains(key)) {
              if (value.equals("-")) {
                avroDataRecord.put(key, null);
              } else {
                avroDataRecord.put(key, Long.parseLong(value));
              }
            } else {
              avroDataRecord.put(key, value);
            }
          } catch (Exception e) {
            avroDataRecord.put(key, null);
          }
        }
        avroDataRecord.put("referrerMap", referrerHeaderList.get(count));
        dataFileWriter.append(avroDataRecord);
        byte[] byteArray = avroDataRecord.toString().getBytes("UTF-8");
        fsDataOutputStream.write(byteArray);
        count += 1;
      }
    }
    dataFileWriter.close();
    LOG.info("Successfully generated avro data");
  }

  /**
   * Merge and parse all the audit log files and convert data into avro file.
   *
   * @param s3AFileSystem filesystem
   * @param s3LogsPath    source path of logs
   * @param s3DestPath    destination path of merged log file
   * @return true
   * @throws IOException on any failure
   */
  public boolean mergeAndParseAuditLogFiles(S3AFileSystem s3AFileSystem,
      Path s3LogsPath,
      Path s3DestPath) throws IOException {

    RemoteIterator<LocatedFileStatus> listOfS3LogFiles =
        s3AFileSystem.listFiles(s3LogsPath, true);

    Path destFile = new Path(s3DestPath, "AuditLogFile");

    List<HashMap<String, String>> entireAuditLogList = new ArrayList<>();
    List<HashMap<String, String>> referrerHeaderList = new ArrayList<>();
    List<HashMap<String, String>> auditLogList = new ArrayList<>();

    try (FSDataOutputStream fsDataOutputStream = s3AFileSystem.create(
        destFile)) {
      // Iterating over the list of files to merge and parse
      while (listOfS3LogFiles.hasNext()) {
        Path logFile = listOfS3LogFiles.next().getPath();
        FileStatus fileStatus = s3AFileSystem.getFileStatus(logFile);
        int fileLength = (int) fileStatus.getLen();
        byte[] byteBuffer = new byte[fileLength];
        try (
            FSDataInputStream fsDataInputStream = s3AFileSystem.open(logFile)) {

          // Reads the file data in byte array
          fsDataInputStream.readFully(byteBuffer);

          // Convert byte array to string to make parsing easy
          String byteStr = new String(byteBuffer, "UTF-8");
          List<String> logs = IOUtils.readLines(new StringReader(byteStr));

          // Parse each and every audit log from list of logs
          for (String singleAuditLog : logs) {
            // Parse audit log except referrer header
            Map<String, String> auditLogMap = parseAuditLog(singleAuditLog);

            String referrerHeader = auditLogMap.get("referrer");
            if (referrerHeader == null || referrerHeader.equals("-")) {
              LOG.info("Log didn't parsed : " + referrerHeader);
              continue;
            }

            // Parse only referrer header
            Map<String, String> referrerHeaderMap =
                parseReferrerHeader(referrerHeader);
            Map<String, String> entireAuditLogMap = new HashMap<>();
            entireAuditLogMap.putAll(auditLogMap);
            entireAuditLogMap.putAll(referrerHeaderMap);

            // Adds every single map containing key-value pairs of single
            // audit log into a list except referrer header key-value pairs.
            auditLogList.add((HashMap<String, String>) auditLogMap);

            // Also adds every single map containing key-value pairs of
            // referrer header into a list.
            referrerHeaderList.add((HashMap<String, String>) referrerHeaderMap);

            // And adds every single map containing key-value pairs of entire
            // audit log into a list including referrer header key-value pairs.
            entireAuditLogList.add((HashMap<String, String>) entireAuditLogMap);
          }
        }
        // Write byte array into a file in destination path.
        fsDataOutputStream.write(byteBuffer);
      }
      // This method is used to convert the list of maps into avro file
      // for querying using hive and spark.
      convertToAvroFile(referrerHeaderList, auditLogList, s3AFileSystem,
          s3DestPath);
    }
    return true;
  }
}
