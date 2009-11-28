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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Input split class for reading data from Vertica
 * 
 */
public class VerticaInputSplit extends InputSplit implements Writable {
  private static final Log LOG = LogFactory.getLog(VerticaInputSplit.class);

  PreparedStatement stmt = null;
  Connection connection = null;
  VerticaConfiguration vtconfig = null;
  String inputQuery = null;
  List<Object> segmentParams = null;
  long start = 0;
  long end = 0;

  /** (@inheritDoc) */
  public VerticaInputSplit() {
    LOG.trace("Input split default constructor");
  }

  /**
   * Set the input query and a list of parameters to substitute when evaluating
   * the query
   * 
   * @param inputQuery
   *          SQL query to run
   * @param segmentParams
   *          list of parameters to substitute into the query
   * @param start
   *          the logical starting record number
   * @param end
   *          the logical ending record number
   */
  public VerticaInputSplit(String inputQuery, List<Object> segmentParams,
      long start, long end) {
    LOG.trace("Input split constructor with query and params");
    this.inputQuery = inputQuery;
    this.segmentParams = segmentParams;
    this.start = start;
    this.end = end;
  }

  /** (@inheritDoc) */
  public void configure(Configuration conf) throws Exception {
    LOG.trace("Input split configured");
    vtconfig = new VerticaConfiguration(conf);
    connection = vtconfig.getConnection(false);
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  /**
   * Return the parameters used for input query
   * 
   * @return
   */
  public List<Object> getSegmentParams() {
    return segmentParams;
  }

  /**
   * Run the query that, when executed returns input for the mapper
   * 
   * @return
   * @throws Exception
   */
  public ResultSet executeQuery() throws Exception {
    LOG.trace("Input split execute query");
    long length = getLength();

    if (length != 0)
      inputQuery = "SELECT * FROM ( " + inputQuery
          + " ) limited LIMIT ? OFFSET ?";

    if (connection == null)
      throw new Exception("Cannot execute query with no connection");
    stmt = connection.prepareStatement(inputQuery);

    int i = 1;
    if (segmentParams != null)
      for (Object param : segmentParams)
        stmt.setObject(i++, param);

    if (length != 0) {
      stmt.setLong(i++, length);
      stmt.setLong(i++, start);
    }

    ResultSet rs = stmt.executeQuery();
    return rs;
  }

  /** (@inheritDoc) */
  public void close() throws SQLException {
    stmt.close();
  }

  /**
   * @return The index of the first row to select
   */
  public long getStart() {
    return start;
  }

  /**
   * @return The index of the last row to select
   */
  public long getEnd() {
    return end;
  }

  /**
   * @return The total row count in this split
   */
  public long getLength() throws IOException {
    // TODO: figureout how to return length when there is no start and end
    return end - start;
  }

  /** {@inheritDoc} */
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  /** (@inheritDoc) */
  public Configuration getConfiguration() {
    return vtconfig.getConfiguration();
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    inputQuery = Text.readString(in);
    long paramCount = in.readLong();
    if (paramCount > 0) {
      VerticaRecord record = new VerticaRecord();
      record.readFields(in);
      segmentParams = record.getValues();
    }
    start = in.readLong();
    end = in.readLong();
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, inputQuery);
    if (segmentParams != null && segmentParams.size() > 0) {
      out.writeLong(segmentParams.size());
      VerticaRecord record = new VerticaRecord(segmentParams, true);
      record.write(out);
    } else
      out.writeLong(0);
    out.writeLong(start);
    out.writeLong(end);
  }
}
