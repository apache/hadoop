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
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaRecordWriter extends RecordWriter<Text, VerticaRecord> {
  String writerTable = null;
  Connection connection = null;
  Statement statement = null; // com.vertica.PGStatement
  String copyStmt = null;
  String delimiter = VerticaConfiguration.DELIMITER;
  String terminator = VerticaConfiguration.RECORD_TERMINATER;

  // Methods from com.vertica.PGStatement
  Method startCopyIn = null;
  Method finishCopyIn = null;
  Method addStreamToCopyIn = null;

  public VerticaRecordWriter(Connection connection, String copyStmt,
      String writerTable, String delimiter, String terminator) {
    this.connection = connection;
    this.copyStmt = copyStmt;
    this.writerTable = writerTable;
    this.delimiter = delimiter;
    this.terminator = terminator;

    try {
      startCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "startCopyIn", String.class, InputStream.class);
      finishCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "finishCopyIn");
      addStreamToCopyIn = Class.forName("com.vertica.PGStatement").getMethod(
          "addStreamToCopyIn", InputStream.class);
    } catch (Exception e) {
      throw new RuntimeException(
          "Vertica Formatter requies a the Vertica jdbc driver");
    }
  }

  public VerticaRecord getValue() throws SQLException {
    DatabaseMetaData dbmd = connection.getMetaData();

    String schema = null;
    String table = null;
    String[] schemaTable = writerTable.split("\\.");
    if (schemaTable.length == 2) {
      schema = schemaTable[0];
      table = schemaTable[1];
    } else if (schemaTable.length == 1) {
      table = schemaTable[0];
    } else {
      throw new RuntimeException(
          "Vertica Formatter requires a value output table");
    }

    List<Integer> types = new ArrayList<Integer>();
    List<String> names = new ArrayList<String>();
    ResultSet rs = dbmd.getColumns(null, schema, table, null);
    while (rs.next()) {
      names.add(rs.getString(4));
      types.add(rs.getInt(5));
    }

    VerticaRecord record = new VerticaRecord(names, types);
    return record;
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
  public void write(Text table, VerticaRecord record) throws IOException {
    if (!table.toString().equals(writerTable))
      throw new IOException("Writing to different table " + table.toString()
          + ". Expecting " + writerTable);

    String strRecord = record.toSQLString(delimiter, terminator);

    ByteArrayInputStream bais = new ByteArrayInputStream(strRecord.getBytes());
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
