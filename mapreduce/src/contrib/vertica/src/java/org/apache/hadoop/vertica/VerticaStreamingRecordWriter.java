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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaStreamingRecordWriter extends RecordWriter<Text, Text> {
  private static final Log LOG = LogFactory
      .getLog(VerticaStreamingRecordWriter.class);

  String writerTable = null;
  Connection connection = null;
  Statement statement = null; // com.vertica.PGStatement
  String copyStmt = null;

  // Methods from com.vertica.PGStatement
  Method startCopyIn = null;
  Method finishCopyIn = null;
  Method addStreamToCopyIn = null;

  public VerticaStreamingRecordWriter(Connection connection, String copyStmt,
      String writerTable) {
    this.connection = connection;
    this.copyStmt = copyStmt;
    this.writerTable = writerTable;

    try {
      startCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "startCopyIn", String.class, ByteArrayInputStream.class);
      finishCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "finishCopyIn");
      addStreamToCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "addStreamToCopyIn", ByteArrayInputStream.class);
    } catch (Exception ee) {
      throw new RuntimeException(
          "Vertica Formatter requies the Vertica jdbc driver");
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    try {
      if (statement != null) {
        finishCopyIn.invoke(statement); // statement.finishCopyIn();
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(Text table, Text record) throws IOException {
    if (!table.toString().equals(writerTable))
      throw new IOException("Writing to different table " + table.toString()
          + ". Expecting " + writerTable);

    if(LOG.isDebugEnabled()) {
      LOG.debug("writing " + record.toString());
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(record.getBytes());
    try {
      if (statement == null) {
        statement = connection.createStatement();
        startCopyIn.invoke(statement, copyStmt, bais); // statement.startCopyIn(copyStmt,
                                                        // bais);
      } else
        addStreamToCopyIn.invoke(statement, bais); // statement.addStreamToCopyIn(bais);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
