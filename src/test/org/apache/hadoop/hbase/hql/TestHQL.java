/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.hql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.hql.generated.ParseException;
import org.apache.hadoop.hbase.hql.generated.HQLParser;
import org.apache.hadoop.io.Text;

/**
 * Tests for HQL
 */
public class TestHQL extends HBaseClusterTestCase {
  
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());
  private ByteArrayOutputStream baos;
  private HBaseAdmin admin;
  
  /** constructor */
  public TestHQL() {
    super(1 /*One region server only*/);
  }
  
  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Capture System.out so we can grep for stuff in it.  Have to do it once
    // only because ConsoleTable sets up STDOUT in a static initialization
    this.baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(this.baos));
    this.admin = new HBaseAdmin(this.conf);
  }

  /**
   * Create and then drop a table.
   * Tests also that I can use single or double quotes around table and 
   * column family names.
   * @throws Exception
   */
  public void testCreateDeleteTable() throws Exception {
    final String tableName = getName();
    final String columnFamily = tableName;
    // Create table
    createTable("create table " + tableName + " (" + columnFamily + ");",
      tableName, columnFamily);
    // Try describe
    runCommand("describe " + tableName + ";");
    // Try describe with single quotes
    runCommand("describe '" + tableName + "';");
    // Try describe with double-quotes
    runCommand("describe \"" + tableName + "\";");
    // Try dropping the table.
    dropTable("drop table " + tableName + ";", tableName);
    // Use double-quotes creating table.
    final String dblQuoteSuffix = "DblQuote";
    final String dblQuotedTableName = tableName + dblQuoteSuffix;
    createTable("create table \"" + dblQuotedTableName + "\" (" +
      columnFamily + ");", dblQuotedTableName, columnFamily);
    // Use single-quotes creating table.
    final String sglQuoteSuffix = "SglQuote";
    final String snglQuotedTableName = tableName + sglQuoteSuffix;
    createTable("create table '" + snglQuotedTableName + "' (" +
      columnFamily + ");", snglQuotedTableName, columnFamily);
    // Use double-quotes around columnfamily name.
    final String dblQuotedColumnFamily = columnFamily + dblQuoteSuffix;
    String tmpTableName = tableName + dblQuotedColumnFamily;
    createTable("create table " + tmpTableName + " (\"" +
      dblQuotedColumnFamily + "\");", tmpTableName,
      dblQuotedColumnFamily);
    // Use single-quotes around columnfamily name.
    final String sglQuotedColumnFamily = columnFamily + sglQuoteSuffix;
    tmpTableName = tableName + sglQuotedColumnFamily;
    createTable("create table " + tmpTableName + " ('" +
        sglQuotedColumnFamily + "');", tmpTableName, sglQuotedColumnFamily);
  }

  /**
   * @throws Exception
   */
  public void testInsertSelectDelete() throws Exception {
    final String tableName = getName();
    final String columnFamily = tableName;
    createTable("create table " + tableName + " (" + columnFamily + ");",
      tableName, columnFamily);
    // TODO: Add asserts that inserts, selects and deletes worked.
    runCommand("insert into " + tableName + " (" + columnFamily +
      ") values ('" + columnFamily + "') where row='" + columnFamily + "';");
    // Insert with double-quotes on row.
    runCommand("insert into " + tableName + " (" + columnFamily +
      ") values ('" + columnFamily + "') where row=\"" + columnFamily + "\";");
    // Insert with double-quotes on row and value.
    runCommand("insert into " + tableName + " (" + columnFamily +
      ") values (\"" + columnFamily + "\") where row=\"" + columnFamily +
      "\";");
    runCommand("select \"" + columnFamily + "\" from \"" + tableName +
      "\" where row=\"" + columnFamily + "\";");
    runCommand("delete \"" + columnFamily + ":\" from \"" + tableName +
        "\" where row=\"" + columnFamily + "\";");
  }
  
  private void createTable(final String cmdStr, final String tableName,
    final String columnFamily)
  throws ParseException, IOException {
    // Run create command.
    runCommand(cmdStr);
    // Assert table was created.
    assertTrue(this.admin.tableExists(new Text(tableName)));
    HTableDescriptor [] tables = this.admin.listTables();
    HTableDescriptor td = null;
    for (int i = 0; i < tables.length; i++) {
      if (tableName.equals(tables[i].getName().toString())) {
        td = tables[i];
      }
    }
    assertNotNull(td);
    assertTrue(td.hasFamily(new Text(columnFamily + ":")));
  }
  
  private void dropTable(final String cmdStr, final String tableName)
  throws ParseException, IOException {
    runCommand(cmdStr);
    // Assert its gone
    HTableDescriptor [] tables = this.admin.listTables();
    for (int i = 0; i < tables.length; i++) {
      assertNotSame(tableName, tables[i].getName().toString());
    }
  }
  
  private ReturnMsg runCommand(final String cmdStr)
  throws ParseException, UnsupportedEncodingException {
    LOG.info("Running command: " + cmdStr);
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    TableFormatterFactory tff = new TableFormatterFactory(out, this.conf);
    HQLParser parser = new HQLParser(cmdStr, out, tff.get());
    Command cmd = parser.terminatedCommand();
    ReturnMsg rm = cmd.execute(this.conf);
    dumpStdout();
    return rm;
  }
  
  private void dumpStdout() throws UnsupportedEncodingException {
    LOG.info("STDOUT: " +
      new String(this.baos.toByteArray(), HConstants.UTF8_ENCODING));
    this.baos.reset();
  }
}