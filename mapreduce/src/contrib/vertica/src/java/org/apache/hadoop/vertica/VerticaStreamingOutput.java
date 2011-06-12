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

package org.apache.hadoop.vertica;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VerticaStreamingOutput extends OutputFormat<Text, Text> {
  private static final Log LOG = LogFactory
      .getLog(VerticaStreamingOutput.class);

  String delimiter = VerticaConfiguration.DELIMITER;
  String terminator = VerticaConfiguration.RECORD_TERMINATER;

  public void checkOutputSpecs(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    VerticaUtil.checkOutputSpecs(conf);
    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);
    delimiter = vtconfig.getOutputDelimiter();
    terminator = vtconfig.getOutputRecordTerminator();
    LOG.debug("Vertica output using delimiter '" + delimiter
        + "' and terminator '" + terminator + "'");
  }

  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    Configuration conf = context.getConfiguration();
    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);

    String name = context.getJobName();
    delimiter = vtconfig.getOutputDelimiter();
    terminator = vtconfig.getOutputRecordTerminator();

    // TODO: use explicit date formats
    String table = vtconfig.getOutputTableName();
    String copyStmt = "COPY " + table + " FROM STDIN" + " DELIMITER '"
        + delimiter + "' RECORD TERMINATOR '" + terminator + "' STREAM NAME '"
        + name + "' DIRECT";

    try {
      Connection conn = vtconfig.getConnection(true);
      return new VerticaStreamingRecordWriter(conn, copyStmt, table);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
        context);
  }
}
