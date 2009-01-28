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

package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p> 
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where 
 * key has a type extending DBWritable. Returned {@link RecordWriter} 
 * writes <b>only the key</b> to the database with a batch SQL query.  
 * 
 */
public class DBOutputFormat<K  extends DBWritable, V> 
implements OutputFormat<K,V> {

  private static final Log LOG = LogFactory.getLog(DBOutputFormat.class);

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  protected class DBRecordWriter 
  implements RecordWriter<K, V> {

    private Connection connection;
    private PreparedStatement statement;

    protected DBRecordWriter(Connection connection
        , PreparedStatement statement) throws SQLException {
      this.connection = connection;
      this.statement = statement;
      this.connection.setAutoCommit(false);
    }

    /** {@inheritDoc} */
    public void close(Reporter reporter) throws IOException {
      try {
        statement.executeBatch();
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        }
        catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e.getMessage());
      } finally {
        try {
          statement.close();
          connection.close();
        }
        catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
      }
    }

    /** {@inheritDoc} */
    public void write(K key, V value) throws IOException {
      try {
        key.write(statement);
        statement.addBatch();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Constructs the query used as the prepared statement to insert data.
   * 
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  protected String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");

    return query.toString();
  }

  /** {@inheritDoc} */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
  throws IOException {
  }


  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(FileSystem filesystem,
      JobConf job, String name, Progressable progress) throws IOException {

    DBConfiguration dbConf = new DBConfiguration(job);
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    
    try {
      Connection connection = dbConf.getConnection();
      PreparedStatement statement = null;
  
      statement = connection.prepareStatement(constructQuery(tableName, fieldNames));
      return new DBRecordWriter(connection, statement);
    }
    catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   * 
   * @param job
   *          The job
   * @param tableName
   *          The table to insert data into
   * @param fieldNames
   *          The field names in the table. If unknown, supply the appropriate
   *          number of nulls.
   */
  public static void setOutput(JobConf job, String tableName, String... fieldNames) {
    job.setOutputFormat(DBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job);
    
    dbConf.setOutputTableName(tableName);
    dbConf.setOutputFieldNames(fieldNames);
  }
}
