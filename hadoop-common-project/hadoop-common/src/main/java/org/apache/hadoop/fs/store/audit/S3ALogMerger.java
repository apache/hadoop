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

package org.apache.hadoop.fs.store.audit;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Merger class will merge all the audit logs present in a directory of multiple audit log files into a single audit log file
 */
public class S3ALogMerger {

    private final Logger LOG = Logger.getLogger(S3ALogMerger.class);

    public void mergeFiles(String auditLogsDirectoryPath) throws IOException {
        File auditLogFilesDirectory = new File(auditLogsDirectoryPath);
        String[] auditLogFileNames = auditLogFilesDirectory.list();
        LOG.info("Files to be merged : " + Arrays.toString(auditLogFileNames));

        //Read each audit log file present in directory and writes each and every audit log in it into a single audit log file
        if(auditLogFileNames != null && auditLogFileNames.length != 0) {
            File auditLogFile = new File("AuditLogFile");
            PrintWriter printWriter = new PrintWriter(auditLogFile);
            for (String singleFileName : auditLogFileNames) {
                File file = new File(auditLogFilesDirectory, singleFileName);
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String singleLine = bufferedReader.readLine();
                while (singleLine != null) {
                    printWriter.println(singleLine);
                    singleLine = bufferedReader.readLine();
                }
                printWriter.flush();
            }
            LOG.info("Successfully merged all files from the directory '" + auditLogFilesDirectory.getName() + "'");
        }

    }
}
