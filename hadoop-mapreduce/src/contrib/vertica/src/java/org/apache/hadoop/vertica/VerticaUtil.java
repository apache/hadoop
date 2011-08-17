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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;

public class VerticaUtil {
  private static final Log LOG = LogFactory.getLog(VerticaUtil.class);

  public static int verticaVersion(Configuration conf, boolean output) throws IOException {
    int ver = -1;
    try {
    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);
    Connection conn = vtconfig.getConnection(output);
    DatabaseMetaData dbmd = conn.getMetaData();
    ver = dbmd.getDatabaseMajorVersion() * 100;
    ver += dbmd.getDatabaseMinorVersion();
    } catch(ClassNotFoundException e) { 
      throw new IOException("Vertica Driver required to use Vertica Input or Output Formatters"); 
    } catch (SQLException e) { throw new IOException(e); }
    return ver;
  }
  
  public static void checkOutputSpecs(Configuration conf) throws IOException {
    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);

    String writerTable = vtconfig.getOutputTableName();
    if (writerTable == null)
      throw new IOException("Vertica output requires a table name defined by "
          + VerticaConfiguration.OUTPUT_TABLE_NAME_PROP);
    String[] def = vtconfig.getOutputTableDef();
    boolean dropTable = vtconfig.getDropTable();

    String schema = null;
    String table = null;
    String[] schemaTable = writerTable.split("\\.");
    if (schemaTable.length == 2) {
      schema = schemaTable[0];
      table = schemaTable[1];
    } else
      table = schemaTable[0];

    Statement stmt = null;
    try {
      Connection conn = vtconfig.getConnection(true);
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = dbmd.getTables(null, schema, table, null);
      boolean tableExists = rs.next();

      stmt = conn.createStatement();

      if (tableExists && dropTable) {
        if(verticaVersion(conf, true) >= 305) {
          stmt = conn.createStatement();
          stmt.execute("TRUNCATE TABLE " + writerTable);
        } else {
          // for version < 3.0 drop the table if it exists
          // if def is empty, grab the columns first to redfine the table
          if (def == null) {
            rs = dbmd.getColumns(null, schema, table, null);
            ArrayList<String> defs = new ArrayList<String>();
            while (rs.next())
              defs.add(rs.getString(4) + " " + rs.getString(5));
            def = defs.toArray(new String[0]);
          }
  
          stmt = conn.createStatement();
          stmt.execute("DROP TABLE " + writerTable + " CASCADE");
          tableExists = false; // force create
        }
      }

      // create table if it doesn't exist
      if (!tableExists) {
        if (def == null)
          throw new RuntimeException("Table " + writerTable
              + " does not exist and no table definition provided");
        if (schema != null) {
          rs = dbmd.getSchemas(null, schema);
          if (!rs.next())
            stmt.execute("CREATE SCHEMA " + schema);
        }
        StringBuffer tabledef = new StringBuffer("CREATE TABLE ").append(
            writerTable).append(" (");
        for (String column : def)
          tabledef.append(column).append(",");
        tabledef.replace(tabledef.length() - 1, tabledef.length(), ")");

        stmt.execute(tabledef.toString());
        // TODO: create segmented projections
        stmt.execute("select implement_temp_design('" + writerTable + "')");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (stmt != null)
        try {
          stmt.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
    }
  }

  // TODO: catch when params required but missing
  // TODO: better error message when count query is bad
  public static List<InputSplit> getSplits(JobContext context)
      throws IOException {
    Configuration conf = context.getConfiguration();
    int numSplits = conf.getInt("mapreduce.job.maps", 1);
    LOG.debug("creating splits up to " + numSplits);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int i = 0;
    long start = 0;
    long end = 0;
    boolean limitOffset = true;

    // This is the fancy part of mapping inputs...here's how we figure out
    // splits
    // get the params query or the params
    VerticaConfiguration config = new VerticaConfiguration(conf);
    String inputQuery = config.getInputQuery();

    if (inputQuery == null)
      throw new IOException("Vertica input requires query defined by "
          + VerticaConfiguration.QUERY_PROP);

    String paramsQuery = config.getParamsQuery();
    Collection<List<Object>> params = config.getInputParameters();

    // TODO: limit needs order by unique key
    // TODO: what if there are more parameters than numsplits?
    // prep a count(*) wrapper query and then populate the bind params for each
    String countQuery = "SELECT COUNT(*) FROM (\n" + inputQuery + "\n) count";

    if (paramsQuery != null) {
      LOG.debug("creating splits using paramsQuery :" + paramsQuery);
      Connection conn = null;
      Statement stmt = null;
      try {
        conn = config.getConnection(false);
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(paramsQuery);
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
          limitOffset = false;
          List<Object> segmentParams = new ArrayList<Object>();
          for (int j = 1; j <= rsmd.getColumnCount(); j++) {
            segmentParams.add(rs.getObject(j));
          }
          splits.add(new VerticaInputSplit(inputQuery, segmentParams, start,
              end));
        }
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        try {
          if (stmt != null)
            stmt.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    } else if (params != null && params.size() > 0) {
      LOG.debug("creating splits using " + params.size() + " params");
      limitOffset = false;
      for (List<Object> segmentParams : params) {
        // if there are more numSplits than params we're going to introduce some
        // limit and offsets
        // TODO: write code to generate the start/end pairs for each group
        splits
            .add(new VerticaInputSplit(inputQuery, segmentParams, start, end));
      }
    }

    if (limitOffset) {
      LOG.debug("creating splits using limit and offset");
      Connection conn = null;
      Statement stmt = null;
      long count = 0;

      try {
        conn = config.getConnection(false);
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(countQuery);
        rs.next();
        count = rs.getLong(1);
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        try {
          if (stmt != null)
            stmt.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }

      long splitSize = count / numSplits;
      end = splitSize;

      LOG.debug("creating " + numSplits + " splits for " + count + " records");

      for (i = 0; i < numSplits; i++) {
        splits.add(new VerticaInputSplit(inputQuery, null, start, end));
        start += splitSize;
        end += splitSize;
      }
    }

    LOG.debug("returning " + splits.size() + " final splits");
    return splits;
  }
}
