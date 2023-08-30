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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.audit.AvroS3LogEntryRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
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

  /**
   *  Max length of a line in the audit log: {@value}.
   */
  public static final int MAX_LINE_LENGTH = 32000;

  /**
   * List of fields in a log record which are of type long.
   */
  public static final List<String> FIELDS_OF_TYPE_LONG =
      Arrays.asList(TURNAROUNDTIME_GROUP, BYTESSENT_GROUP,
          OBJECTSIZE_GROUP, TOTALTIME_GROUP);

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AAuditLogMergerAndParser.class);

  /**
   * Referrer header key from the regexp, not the actual header name
   * which is spelled differently: {@value}.
   */
  private static final String REFERRER_HEADER_KEY = "referrer";

  /**
   * Value to use when a long value cannot be parsed: {@value}.
   */
  public static final long FAILED_TO_PARSE_LONG = -1L;

  /**
   * Key for the referrer map in the Avro record: {@value}.
   */
  public static final String REFERRER_MAP = "referrerMap";

  private final Configuration conf;
  /*
   * Number of records to process before giving a status update.
   */
  private final int sample;

  // Basic parsing counters.

  /**
   * Number of audit log files parsed.
   */
  private long logFilesParsed = 0;

  /**
   * Number of log entries parsed.
   */
  private long auditLogsParsed = 0;

  /**
   * How many referrer headers were parsed.
   */
  private long referrerHeaderLogParsed = 0;

  /**
   * How many records were skipped due to lack of referrer or other reason.
   */
  private long recordsSkipped = 0;

  public S3AAuditLogMergerAndParser(final Configuration conf, final int sample) {
    this.conf = conf;

    this.sample = sample;
  }

  /**
   * parseAuditLog method helps in parsing the audit log
   * into key-value pairs using regular expressions.
   * @param singleAuditLog this is single audit log from merged audit log file
   * @return it returns a map i.e, auditLogMap which contains key-value pairs of a single audit log
   */
  public HashMap<String, String> parseAuditLog(String singleAuditLog) {
    HashMap<String, String> auditLogMap = new HashMap<>();
    if (singleAuditLog == null || singleAuditLog.isEmpty()) {
      LOG.debug("This is an empty string or null string, expected a valid string to parse");
      return auditLogMap;
    }
    final Matcher matcher = LOG_ENTRY_PATTERN.matcher(singleAuditLog);
    boolean patternMatched = matcher.matches();
    if (patternMatched) {
      for (String key : AWS_LOG_REGEXP_GROUPS) {
        try {
          final String value = matcher.group(key);
          auditLogMap.put(key, value);
        } catch (IllegalStateException e) {
          LOG.debug("Skipping key :{} due to no match with the audit log "
              + "pattern :", key);
          LOG.debug(String.valueOf(e));
        }
      }
    }
    return auditLogMap;
  }

  /**
   * Parses the http referrer header.
   * which is one of the key-value pair of audit log.
   * @param referrerHeader this is the http referrer header of a particular
   * audit log.
   * @return returns a map which contains key-value pairs of referrer headers; an empty map
   *         there was no referrer header or parsing failed
   */
  public static HashMap<String, String> parseAudit(String referrerHeader) {
    HashMap<String, String> referrerHeaderMap = new HashMap<>();
    if (StringUtils.isEmpty(referrerHeader)
        || referrerHeader.equals("-")) {

      LOG.debug("This is an empty string or null string, expected a valid string to parse");
      return referrerHeaderMap;
    }

    // '?' used as the split point between the headers and the url. This
    // returns the first occurrence of '?'
    int indexOfQuestionMark = referrerHeader.indexOf("?");
    String httpReferrer = referrerHeader.substring(indexOfQuestionMark + 1,
        referrerHeader.length() - 1);

    int lengthOfReferrer = httpReferrer.length();
    int start = 0;
    LOG.debug("HttpReferrer headers string: {}", httpReferrer);
    while (start < lengthOfReferrer) {
      // splits "key" and "value" of each header
      int equals = httpReferrer.indexOf("=", start);
      // no match : break, no header left
      if (equals == -1) {
        break;
      }
      // key represents the string between "start" and index of "=".
      String key = httpReferrer.substring(start, equals);
      // splits between different headers, this also helps in ignoring "="
      // inside values since we set the end till we find '&'
      int end = httpReferrer.indexOf("&", equals);
      // or end of string
      if (end == -1) {
        end = lengthOfReferrer;
      }
      // value represents the string between index of "=" + 1 and the "end"
      String value = httpReferrer.substring(equals + 1, end);
      referrerHeaderMap.put(key, value);
      start = end + 1;
    }

    return referrerHeaderMap;
  }

  /**
   * Merge and parse all the audit log files and convert data into avro file.
   * @param logsPath source path of logs
   * @param destFile destination path of merged log file
   * @return true
   * @throws IOException on any failure
   */
  public boolean mergeAndParseAuditLogFiles(
      final Path logsPath,
      final Path destFile) throws IOException {

    // List source log files
    final FileSystem sourceFS = logsPath.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> listOfLogFiles =
        sourceFS.listFiles(logsPath, true);

    final FileSystem destFS = destFile.getFileSystem(conf);
    final FSDataOutputStreamBuilder builder = destFS.createFile(destFile)
        .recursive()
        .overwrite(true);

    // this has a broken return type; a java typesystem quirk.
    builder.opt(FS_S3A_CREATE_PERFORMANCE, true);

    FSDataOutputStream fsDataOutputStream = builder.build();

    // Instantiate DatumWriter class
    DatumWriter<AvroS3LogEntryRecord> datumWriter =
        new SpecificDatumWriter<>(
            AvroS3LogEntryRecord.class);
    try (DataFileWriter<AvroS3LogEntryRecord> dataFileWriter =
            new DataFileWriter<>(datumWriter);
         DataFileWriter<AvroS3LogEntryRecord> avroWriter =
             dataFileWriter.create(AvroS3LogEntryRecord.getClassSchema(),
                 fsDataOutputStream);) {

      // Iterating over the list of files to merge and parse
      while (listOfLogFiles.hasNext()) {
        logFilesParsed++;
        FileStatus fileStatus = listOfLogFiles.next();
        int fileLength = (int) fileStatus.getLen();

        try (DurationInfo duration = new DurationInfo(LOG, "Processing %s", fileStatus.getPath());
             FSDataInputStream fsDataInputStream =
                 awaitFuture(sourceFS.openFile(fileStatus.getPath())
                     .withFileStatus(fileStatus)
                     .opt(FS_OPTION_OPENFILE_READ_POLICY,
                         FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
                     .build())) {

          // Reading the file data using LineRecordReader
          LineRecordReader lineRecordReader =
              new LineRecordReader(fsDataInputStream, 0L, fileLength,
                  MAX_LINE_LENGTH);
          LongWritable longWritable = new LongWritable();
          Text singleAuditLog = new Text();

          // Parse each and every audit log from list of logs
          while (lineRecordReader.next(longWritable, singleAuditLog)) {
            // Parse audit log
            HashMap<String, String> auditLogMap =
                parseAuditLog(singleAuditLog.toString());
            auditLogsParsed++;

            // Insert data according to schema

            // Instantiating generated AvroDataRecord class
            AvroS3LogEntryRecord avroDataRecord = buildLogRecord(auditLogMap);
            if (!avroDataRecord.getReferrerMap().isEmpty()) {
              referrerHeaderLogParsed++;
            }
            avroWriter.append(avroDataRecord);
          }
          dataFileWriter.flush();
        }
      }
    }

    LOG.info("Successfully parsed :{} audit logs and {} referrer header "
        + "in the logs", auditLogsParsed, referrerHeaderLogParsed);
    return true;
  }

  public long getAuditLogsParsed() {
    return auditLogsParsed;
  }

  public long getReferrerHeaderLogParsed() {
    return referrerHeaderLogParsed;
  }

  /**
   * Build log record from a parsed audit log entry.
   *
   * @param auditLogMap parsed audit log entry.
   * @return the Avro record.
   */
  public AvroS3LogEntryRecord buildLogRecord(Map<String, String> auditLogMap) {

    // Instantiating generated AvroDataRecord class
    AvroS3LogEntryRecord avroDataRecord = new AvroS3LogEntryRecord();
    for (Map.Entry<String, String> entry : auditLogMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue().trim();

      // if value == '-' and key is not in arraylist then put '-' or else '-1'
      // if key is in arraylist of long values then parse the long value
      // while parsing do it in try-catch block,
      // in catch block need to log exception and set value as '-1'
      try {
        if (FIELDS_OF_TYPE_LONG.contains(key)) {
          if (value.equals("-")) {
            avroDataRecord.put(key, null);
          } else {
            try {
              avroDataRecord.put(key, Long.parseLong(value));
            } catch (NumberFormatException e) {
              // failed to parse the long value.
              LOG.debug("Failed to parse long value for key {} : {}", key, value);
              avroDataRecord.put(key, FAILED_TO_PARSE_LONG);
            }
          }
        } else {
          avroDataRecord.put(key, value);
        }
      } catch (Exception e) {
        avroDataRecord.put(key, null);
      }
    }

    // Parse the audit header
    HashMap<String, String> referrerHeaderMap =
        parseAudit(auditLogMap.get(REFERRER_HEADER_KEY));
    avroDataRecord.put(REFERRER_MAP, referrerHeaderMap);
    return avroDataRecord;
  }
}
