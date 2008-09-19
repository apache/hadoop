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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as 
 * key and DBWritables as value. 
 * 
 * The SQL query, and input class can be using one of the two 
 * setInput methods.
 */
public class DBInputFormat<T  extends DBWritable>
  implements InputFormat<LongWritable, T>, JobConfigurable {
  /**
   * A RecordReader that reads records from a SQL table.
   * Emits LongWritables containing the record number as 
   * key and DBWritables as value.  
   */
  protected class DBRecordReader implements
  RecordReader<LongWritable, T> {
    private ResultSet results;

    private Statement statement;

    private Class<T> inputClass;

    private JobConf job;

    private DBInputSplit split;

    private long pos = 0;

    /**
     * @param split The InputSplit to read data for
     * @throws SQLException 
     */
    protected DBRecordReader(DBInputSplit split, Class<T> inputClass, JobConf job) throws SQLException {
      this.inputClass = inputClass;
      this.split = split;
      this.job = job;
      
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      //statement.setFetchSize(Integer.MIN_VALUE);
      results = statement.executeQuery(getSelectQuery());
    }

    /** Returns the query for selecting the records, 
     * subclasses can override this for custom behaviour.*/
    protected String getSelectQuery() {
      StringBuilder query = new StringBuilder();
      
      if(dbConf.getInputQuery() == null) {
        query.append("SELECT ");

        for (int i = 0; i < fieldNames.length; i++) {
          query.append(fieldNames[i]);
          if(i != fieldNames.length -1) {
            query.append(", ");
          }
        }

        query.append(" FROM ").append(tableName);
        query.append(" AS ").append(tableName); //in hsqldb this is necessary
        if (conditions != null && conditions.length() > 0)
          query.append(" WHERE (").append(conditions).append(")");
        String orderBy = dbConf.getInputOrderBy();
        if(orderBy != null && orderBy.length() > 0) {
          query.append(" ORDER BY ").append(orderBy);
        }
      }
      else {
        query.append(dbConf.getInputQuery());
      }

      try {
        query.append(" LIMIT ").append(split.getLength());
        query.append(" OFFSET ").append(split.getStart());
      }
      catch (IOException ex) {
        //ignore, will not throw
      }
      return query.toString();
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      try {
        connection.commit();
        results.close();
        statement.close();
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
    }

    /** {@inheritDoc} */
    public LongWritable createKey() {
      return new LongWritable();  
    }

    /** {@inheritDoc} */
    public T createValue() {
      return ReflectionUtils.newInstance(inputClass, job);
    }

    /** {@inheritDoc} */
    public long getPos() throws IOException {
      return pos;
    }

    /** {@inheritDoc} */
    public float getProgress() throws IOException {
      return pos / (float)split.getLength();
    }

    /** {@inheritDoc} */
    public boolean next(LongWritable key, T value) throws IOException {
      try {
        if (!results.next())
          return false;

        // Set the key field value as the output key value
        key.set(pos + split.getStart());

        value.readFields(results);

        pos ++;
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
      return true;
    }
  }

  /**
   * A Class that does nothing, implementing DBWritable
   */
  public static class NullDBWritable implements DBWritable, Writable {
    @Override
    public void readFields(DataInput in) throws IOException { }
    @Override
    public void readFields(ResultSet arg0) throws SQLException { }
    @Override
    public void write(DataOutput out) throws IOException { }
    @Override
    public void write(PreparedStatement arg0) throws SQLException { }
  }
  /**
   * A InputSplit that spans a set of rows
   */
  protected static class DBInputSplit implements InputSplit {

    private long end = 0;
    private long start = 0;

    /**
     * Default Constructor
     */
    public DBInputSplit() {
    }

    /**
     * Convenience Constructor
     * @param start the index of the first row to select
     * @param end the index of the last row to select
     */
    public DBInputSplit(long start, long end) {
      this.start = start;
      this.end = end;
    }

    /** {@inheritDoc} */
    public String[] getLocations() throws IOException {
      // TODO Add a layer to enable SQL "sharding" and support locality
      return new String[] {};
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
      return end - start;
    }

    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      start = input.readLong();
      end = input.readLong();
    }

    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      output.writeLong(start);
      output.writeLong(end);
    }
  }

  private String conditions;

  private Connection connection;

  private String tableName;

  private String[] fieldNames;

  private DBConfiguration dbConf;

  /** {@inheritDoc} */
  public void configure(JobConf job) {

    dbConf = new DBConfiguration(job);

    try {
      this.connection = dbConf.getConnection();
      this.connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    tableName = dbConf.getInputTableName();
    fieldNames = dbConf.getInputFieldNames();
    conditions = dbConf.getInputConditions();
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  public RecordReader<LongWritable, T> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    Class inputClass = dbConf.getInputClass();
    try {
      return new DBRecordReader((DBInputSplit) split, inputClass, job);
    }
    catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /** {@inheritDoc} */
  public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {

    try {
      Statement statement = connection.createStatement();

      ResultSet results = statement.executeQuery(getCountQuery());
      results.next();

      long count = results.getLong(1);
      long chunkSize = (count / chunks);

      results.close();
      statement.close();

      InputSplit[] splits = new InputSplit[chunks];

      // Split the rows into n-number of chunks and adjust the last chunk
      // accordingly
      for (int i = 0; i < chunks; i++) {
        DBInputSplit split;

        if ((i + 1) == chunks)
          split = new DBInputSplit(i * chunkSize, count);
        else
          split = new DBInputSplit(i * chunkSize, (i * chunkSize)
              + chunkSize);

        splits[i] = split;
      }

      return splits;
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  /** Returns the query for getting the total number of rows, 
   * subclasses can override this for custom behaviour.*/
  protected String getCountQuery() {
    
    if(dbConf.getInputCountQuery() != null) {
      return dbConf.getInputCountQuery();
    }
    
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM " + tableName);

    if (conditions != null && conditions.length() > 0)
      query.append(" WHERE " + conditions);
    return query.toString();
  }

  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param tableName The table to read data from
   * @param conditions The condition which to select data with, eg. '(updated >
   * 20070101 AND length > 0)'
   * @param orderBy the fieldNames in the orderBy clause.
   * @param fieldNames The field names in the table
   * @see #setInput(JobConf, Class, String, String)
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String tableName,String conditions, String orderBy, String... fieldNames) {
    job.setInputFormat(DBInputFormat.class);

    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    dbConf.setInputFieldNames(fieldNames);
    dbConf.setInputConditions(conditions);
    dbConf.setInputOrderBy(orderBy);
  }
  
  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param inputQuery the input query to select fields. Example : 
   * "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
   * @param inputCountQuery the input query that returns the number of records in
   * the table. 
   * Example : "SELECT COUNT(f1) FROM Mytable"
   * @see #setInput(JobConf, Class, String, String, String, String...)
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String inputQuery, String inputCountQuery) {
    job.setInputFormat(DBInputFormat.class);
    
    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
    
  }
}
