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

package org.apache.hadoop.fs.s3a.audit.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.audit.AvroS3LogEntryRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;

import static org.apache.hadoop.fs.s3a.audit.S3LogParser.AWS_LOG_REGEXP_GROUPS;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.BYTESSENT_GROUP;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.LOG_ENTRY_PATTERN;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.OBJECTSIZE_GROUP;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.TOTALTIME_GROUP;
import static org.apache.hadoop.fs.s3a.audit.S3LogParser.TURNAROUNDTIME_GROUP;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Merge all the audit logs present in a directory of
 * multiple audit log files into a single audit log file.
 */
public class S3AAuditLogMergerAndParser {

  public static final int MAX_LINE_LENGTH = 32000;
  private static final Logger LOG =
      LoggerFactory.getLogger(S3AAuditLogMergerAndParser.class);

  private long auditLogsParsed = 0;

  /**
   * parseAuditLog method helps in parsing the audit log
   * into key-value pairs using regular expressions.
   *
   * @param singleAuditLog this is single audit log from merged audit log file
   * @return it returns a map i.e, auditLogMap which contains key-value pairs of a single audit log
   */
  public HashMap<String, String> parseAuditLog(String singleAuditLog) {
    HashMap<String, String> auditLogMap = new HashMap<>();
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
          LOG.debug(String.valueOf(e));
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
  public HashMap<String, String> parseReferrerHeader(String referrerHeader) {
    HashMap<String, String> referrerHeaderMap = new HashMap<>();
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
   * Merge and parse all the audit log files and convert data into avro file.
   *
   * @param fileSystem filesystem
   * @param logsPath   source path of logs
   * @param destPath   destination path of merged log file
   * @return true
   * @throws IOException on any failure
   */
  public boolean mergeAndParseAuditLogFiles(FileSystem fileSystem,
      Path logsPath,
      Path destPath) throws IOException {

    // Listing file in given path
    RemoteIterator<LocatedFileStatus> listOfLogFiles =
        fileSystem.listFiles(logsPath, true);

    Path destFile = new Path(destPath, "AuditLogFile");

    try (FSDataOutputStream fsDataOutputStream = fileSystem.create(destFile)) {

      // Iterating over the list of files to merge and parse
      while (listOfLogFiles.hasNext()) {
        FileStatus fileStatus = listOfLogFiles.next();
        int fileLength = (int) fileStatus.getLen();
        byte[] byteBuffer = new byte[fileLength];

        try (FSDataInputStream fsDataInputStream =
            awaitFuture(fileSystem.openFile(fileStatus.getPath())
                .withFileStatus(fileStatus)
                .build())) {

          // Instantiating generated AvroDataRecord class
          AvroS3LogEntryRecord avroDataRecord = new AvroS3LogEntryRecord();

          // Instantiate DatumWriter class
          DatumWriter<AvroS3LogEntryRecord> datumWriter =
              new SpecificDatumWriter<AvroS3LogEntryRecord>(AvroS3LogEntryRecord.class);
          DataFileWriter<AvroS3LogEntryRecord> dataFileWriter =
              new DataFileWriter<AvroS3LogEntryRecord>(datumWriter);

          List<String> longValues =
              Arrays.asList(TURNAROUNDTIME_GROUP, BYTESSENT_GROUP,
                  OBJECTSIZE_GROUP, TOTALTIME_GROUP);

          // Write avro data into a file in bucket destination path
          Path avroFile = new Path(destPath, "AvroData.avro");

          // Reading the file data using LineRecordReader
          LineRecordReader lineRecordReader =
              new LineRecordReader(fsDataInputStream, 0L, fileLength,
                  MAX_LINE_LENGTH);
          LongWritable k = new LongWritable();
          Text singleAuditLog = new Text();

          try (FSDataOutputStream fsDataOutputStreamAvro = fileSystem.create(
              avroFile)) {
            // adding schema, output stream to DataFileWriter
            dataFileWriter.create(AvroS3LogEntryRecord.getClassSchema(),
                fsDataOutputStreamAvro);

            // Parse each and every audit log from list of logs
            while (lineRecordReader.next(k, singleAuditLog)) {
              // Parse audit log except referrer header
              HashMap<String, String> auditLogMap =
                  parseAuditLog(singleAuditLog.toString());

              String referrerHeader = auditLogMap.get("referrer");
              if (referrerHeader == null || referrerHeader.equals("-")) {
                LOG.debug("Log didn't parse : {}", referrerHeader);
                continue;
              }

              // Parse only referrer header
              HashMap<String, String> referrerHeaderMap =
                  parseReferrerHeader(referrerHeader);

              if (referrerHeaderMap.size() > 0) {
                auditLogsParsed++;
              }

              // Insert data according to schema
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
              avroDataRecord.put("referrerMap", referrerHeaderMap);
              dataFileWriter.append(avroDataRecord);
            }
            dataFileWriter.flush();
          }
        }
        // Write byte array into a file in destination path.
        fsDataOutputStream.write(byteBuffer);
      }
      LOG.info("Successfully generated avro data");
    }
    return true;
  }

  public long getAuditLogsParsed() {
    return auditLogsParsed;
  }
}
