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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Output formatter for loading reducer output to Vertica
 * 
 */
public class VerticaOutputFormat extends OutputFormat<Text, VerticaRecord> {
  String delimiter = VerticaConfiguration.DELIMITER;
  String terminator = VerticaConfiguration.RECORD_TERMINATER;

  /**
   * Set the output table
   * 
   * @param conf
   * @param tableName
   */
  public static void setOutput(Job job, String tableName) {
    setOutput(job, tableName, false);
  }

  /**
   * Set the output table and whether to drop it before loading
   * 
   * @param job
   * @param tableName
   * @param dropTable
   */
  public static void setOutput(Job job, String tableName, boolean dropTable) {
    setOutput(job, tableName, dropTable, (String[])null);
  }

  /**
   * Set the output table, whether to drop it before loading and the create
   * table specification if it doesn't exist
   * 
   * @param job
   * @param tableName
   * @param dropTable
   * @param tableDef
   *          list of column definitions such as "foo int", "bar varchar(10)"
   */
  public static void setOutput(Job job, String tableName, boolean dropTable,
      String... tableDef) {
    VerticaConfiguration vtconfig = new VerticaConfiguration(job
        .getConfiguration());
    vtconfig.setOutputTableName(tableName);
    vtconfig.setOutputTableDef(tableDef);
    vtconfig.setDropTable(dropTable);
  }

  // TODO: handle collection of output tables private class VerticaTable {

  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context) throws IOException {
    VerticaUtil.checkOutputSpecs(context.getConfiguration());
    VerticaConfiguration vtconfig = new VerticaConfiguration(context
        .getConfiguration());
    delimiter = vtconfig.getOutputDelimiter();
    terminator = vtconfig.getOutputRecordTerminator();
  }

  /**
   * Test check specs (don't connect to db)
   * 
   * @param context
   * @param test
   *          true if testing
   * @throws IOException
   */
  public void checkOutputSpecs(JobContext context, boolean test)
      throws IOException {
    VerticaUtil.checkOutputSpecs(context.getConfiguration());
    VerticaConfiguration vtconfig = new VerticaConfiguration(context
        .getConfiguration());
    delimiter = vtconfig.getOutputDelimiter();
    terminator = vtconfig.getOutputRecordTerminator();
  }

  /** {@inheritDoc} */
  public RecordWriter<Text, VerticaRecord> getRecordWriter(
      TaskAttemptContext context) throws IOException {

    VerticaConfiguration config = new VerticaConfiguration(context
        .getConfiguration());

    String name = context.getJobName();
    // TODO: use explicit date formats
    String table = config.getOutputTableName();
    String copyStmt = "COPY " + table + " FROM STDIN" + " DELIMITER '"
        + delimiter + "' RECORD TERMINATOR '" + terminator + "' STREAM NAME '"
        + name + "' DIRECT";

    try {
      Connection conn = config.getConnection(true);
      return new VerticaRecordWriter(conn, copyStmt, table, delimiter,
          terminator);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** {@inheritDoc} */
  public static VerticaRecord getValue(Configuration conf) throws Exception {
    VerticaConfiguration config = new VerticaConfiguration(conf);
    String table = config.getOutputTableName();
    Connection conn = config.getConnection(true);
    return (new VerticaRecordWriter(conn, "", table, config
        .getOutputDelimiter(), config.getOutputRecordTerminator())).getValue();
  }

  /**
   * Optionally called at the end of a job to optimize any newly created and
   * loaded tables. Useful for new tables with more than 100k records.
   * 
   * @param conf
   * @throws Exception
   */
  public static void optimize(Configuration conf) throws Exception {
    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);
    Connection conn = vtconfig.getConnection(true);

    // TODO: consider more tables and skip tables with non-temp projections 
    String tableName = vtconfig.getOutputTableName();
    Statement stmt = conn.createStatement();
    ResultSet rs = null;
    StringBuffer designTables = new StringBuffer(tableName);
    HashSet<String> tablesWithTemp = new HashSet<String>();

    //fully qualify the table name - defaults to public.<table>
    if(tableName.indexOf(".") == -1) {
      tableName = "public." + tableName;
    }

    //for now just add the single output table
    tablesWithTemp.add(tableName);

    // map from table name to set of projection names
    HashMap<String, Collection<String>> tableProj = new HashMap<String, Collection<String>>();
    rs = stmt.executeQuery("select schemaname, anchortablename, projname from vt_projection;");
    while(rs.next()) {
      String ptable = rs.getString(1) + "." + rs.getString(2);
      if(!tableProj.containsKey(ptable)) {
        tableProj.put(ptable, new HashSet<String>());
      }
      
      tableProj.get(ptable).add(rs.getString(3));
    }
    
    for(String table : tablesWithTemp) {
      if(!tableProj.containsKey(table)) {
        throw new RuntimeException("Cannot optimize table with no data: " + table);
      }
    }
    
    String designName = (new Integer(conn.hashCode())).toString();
    stmt.execute("select create_projection_design('" + designName + "', '', '"
        + designTables.toString() + "')");

    if(VerticaUtil.verticaVersion(conf, true) >= VerticaConfiguration.VERSION_3_5) {
      stmt.execute("select deploy_design('" + designName + "', '" + designName + "')");
    } else {
      rs = stmt.executeQuery("select get_design_script('" + designName + "', '"
          + designName + "')");
      rs.next();
      String[] projSet = rs.getString(1).split(";");
      for (String proj : projSet) {
        stmt.execute(proj);
      }
      stmt.execute("select start_refresh()");
  
      // poll for refresh complete
      boolean refreshing = true;
      Long timeout = vtconfig.getOptimizePollTimeout();
      while (refreshing) {
        refreshing = false;
        rs = stmt
            .executeQuery("select table_name, status from vt_projection_refresh");
        while (rs.next()) {
          String table = rs.getString(1);
          String stat = rs.getString(2);
          if (stat.equals("refreshing") && tablesWithTemp.contains(table))
            refreshing = true;
        }
        rs.close();
  
        Thread.sleep(timeout);
      }
  
      // refresh done, move the ancient history mark (ahm) and drop the temp projections
      stmt.execute("select make_ahm_now()");
  
      for (String table : tablesWithTemp) {
        for (String proj : tableProj.get(table)) {
          stmt.execute("DROP PROJECTION " + proj);
        }
      }

      stmt.close();
    }
  }

  /** (@inheritDoc) */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
        context);
  }
}