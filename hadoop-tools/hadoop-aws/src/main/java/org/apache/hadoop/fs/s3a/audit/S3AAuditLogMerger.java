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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merge all the audit logs present in a directory of.
 * multiple audit log files into a single audit log file.
 */
public class S3AAuditLogMerger {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AAuditLogMerger.class);

  /**
   * Merge all the audit log files from a directory into single audit log file.
   * @param auditLogsDirectoryPath path where audit log files are present.
   * @throws IOException on any failure.
   */
  public void mergeFiles(String auditLogsDirectoryPath) throws IOException {
    File auditLogFilesDirectory = new File(auditLogsDirectoryPath);
    String[] auditLogFileNames = auditLogFilesDirectory.list();

    //Merging of audit log files present in a directory into a single audit log file
    if (auditLogFileNames != null && auditLogFileNames.length != 0) {
      File auditLogFile = new File("AuditLogFile");
      try (PrintWriter printWriter = new PrintWriter(auditLogFile,
          "UTF-8")) {
        for (String singleAuditLogFileName : auditLogFileNames) {
          File singleAuditLogFile =
              new File(auditLogFilesDirectory, singleAuditLogFileName);
          try (BufferedReader bufferedReader =
              new BufferedReader(
                  new InputStreamReader(new FileInputStream(singleAuditLogFile),
                      "UTF-8"))) {
            String singleLine = bufferedReader.readLine();
            while (singleLine != null) {
              printWriter.println(singleLine);
              singleLine = bufferedReader.readLine();
            }
            printWriter.flush();
          }
        }
      }
      LOG.info("Successfully merged all audit log files from '{}' directory",
          auditLogFilesDirectory.getName());
    } else {
      LOG.info(
          "'{}' is an empty directory, expecting a directory with audit log "
              + "files", auditLogFilesDirectory.getName());
    }
  }
}
