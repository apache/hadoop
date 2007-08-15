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

// temporary until I change all the classes that depend on HClient.
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.io.Text;

/**
 * The HClient class is deprecated and is now implemented entirely in terms of
 * the classes that replace it:
 * <ul>
 * <li>HConnection which manages connections to a a single HBase instance</li>
 * <li>HTable which accesses one table (to access multiple tables, you create
 * multiple HTable instances</li>
 * <li>HBaseAdmin which performs administrative functions for a single HBase
 * instance</li>
 * </ul>
 * <p>
 * HClient continues to be supported in the short term to give users a chance
 * to migrate to the use of HConnection, HTable and HBaseAdmin. Any new API
 * features which are added will be added to these three classes and will not
 * be supported in HClient.
 */
@Deprecated
public class HClient implements HConstants {
  private final Configuration conf;
  protected AtomicReference<HConnection> connection;
  protected AtomicReference<HBaseAdmin> admin;
  protected AtomicReference<HTable> table;

  /** 
   * Creates a new HClient
   * @param conf - Configuration object
   */
  public HClient(Configuration conf) {
    this.conf = conf;
    this.connection = new AtomicReference<HConnection>();
    this.admin = new AtomicReference<HBaseAdmin>();
    this.table = new AtomicReference<HTable>();
  }

  /* Lazily creates a HConnection */
  private synchronized HConnection getHConnection() {
    HConnection conn = connection.get();
    if (conn == null) {
      conn = HConnectionManager.getConnection(conf);
      connection.set(conn);
    }
    return conn;
  }
  
  /* Lazily creates a HBaseAdmin */
  private synchronized HBaseAdmin getHBaseAdmin() throws MasterNotRunningException {
    getHConnection();                   // ensure we have a connection
    HBaseAdmin adm = admin.get();
    if (adm == null) {
      adm = new HBaseAdmin(conf);
      admin.set(adm);
    }
    return adm;
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   */
  protected HRegionLocation getRegionLocation(Text row) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return table.get().getRegionLocation(row);
  }
  
  /** 
   * Establishes a connection to the region server at the specified address.
   * @param regionServer - the server to connect to
   * @throws IOException
   */
  protected HRegionInterface getHRegionConnection(
      HServerAddress regionServer) throws IOException {
    return getHConnection().getHRegionConnection(regionServer);
  }
  
  /**
   * @return - true if the master server is running
   */
  public boolean isMasterRunning() {
    return getHConnection().isMasterRunning();
  }

  //
  // Administrative methods
  //

  /**
   * Creates a new table
   * 
   * @param desc table descriptor for table
   * 
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws NoServerForRegionException if root region is not being served
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    
    getHBaseAdmin().createTable(desc);
  }
  
  /**
   * Creates a new table but does not block and wait for it to come online.
   * 
   * @param desc table descriptor for table
   * 
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws NoServerForRegionException if root region is not being served
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc)
      throws IOException {

    getHBaseAdmin().createTableAsync(desc);
  }

  /**
   * Deletes a table
   * 
   * @param tableName name of table to delete
   * @throws IOException
   */
  public void deleteTable(Text tableName) throws IOException {
    getHBaseAdmin().deleteTable(tableName);
  }

  /**
   * Brings a table on-line (enables it)
   * 
   * @param tableName name of the table
   * @throws IOException
   */
  public void enableTable(Text tableName) throws IOException {
    getHBaseAdmin().enableTable(tableName);
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * 
   * @param tableName name of table
   * @throws IOException
   */
  public void disableTable(Text tableName) throws IOException {
    getHBaseAdmin().disableTable(tableName);
  }
  
  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException
   */
  public boolean tableExists(final Text tableName) throws MasterNotRunningException {
    return getHBaseAdmin().tableExists(tableName);
  }

  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
   */
  public void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {
    getHBaseAdmin().addColumn(tableName, column);
  }

  /**
   * Delete a column from a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException
   */
  public void deleteColumn(Text tableName, Text columnName)
  throws IOException {
    getHBaseAdmin().deleteColumn(tableName, columnName);
  }
  
  /** 
   * Shuts down the HBase instance 
   * @throws IOException
   */
  public void shutdown() throws IOException {
    getHBaseAdmin().shutdown();
  }

  //////////////////////////////////////////////////////////////////////////////
  // Client API
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Loads information so that a table can be manipulated.
   * 
   * @param tableName the table to be located
   * @throws IOException if the table can not be located after retrying
   */
  public synchronized void openTable(Text tableName) throws IOException {
    HTable table = this.table.get();
    if (table != null) {
      table.checkUpdateInProgress();
    }
    this.table.set(new HTable(conf, tableName));
  }
  
  /**
   * Gets the starting row key for every region in the currently open table
   * @return Array of region starting row keys
   */
  public Text[] getStartKeys() {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return table.get().getStartKeys();
  }
  
  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors 
   * @throws IOException
   */
  public HTableDescriptor[] listTables() throws IOException {
    return getHConnection().listTables();
  }

  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public byte[] get(Text row, Text column) throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().get(row, column);
  }
 
  /** 
   * Get the specified number of versions of the specified row and column
   * 
   * @param row         - row key
   * @param column      - column name
   * @param numVersions - number of versions to retrieve
   * @return            - array byte values
   * @throws IOException
   */
  public byte[][] get(Text row, Text column, int numVersions)
  throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().get(row, column, numVersions);
  }
  
  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param row         - row key
   * @param column      - column name
   * @param timestamp   - timestamp
   * @param numVersions - number of versions to retrieve
   * @return            - array of values that match the above criteria
   * @throws IOException
   */
  public byte[][] get(Text row, Text column, long timestamp, int numVersions)
  throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().get(row, column, timestamp, numVersions);
  }
    
  /** 
   * Get all the data for the specified row
   * 
   * @param row         - row key
   * @return            - map of colums to values
   * @throws IOException
   */
  public SortedMap<Text, byte[]> getRow(Text row) throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().getRow(row);
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public HScannerInterface obtainScanner(Text[] columns,
      Text startRow) throws IOException {
    return obtainScanner(columns, startRow, System.currentTimeMillis(), null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public HScannerInterface obtainScanner(Text[] columns,
      Text startRow, long timestamp) throws IOException {
    return obtainScanner(columns, startRow, timestamp, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public HScannerInterface obtainScanner(Text[] columns,
      Text startRow, RowFilterInterface filter) throws IOException { 
    return obtainScanner(columns, startRow, System.currentTimeMillis(), filter);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public HScannerInterface obtainScanner(Text[] columns,
      Text startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().obtainScanner(columns, startRow, timestamp, filter);
  }

  /** 
   * Start a batch of row insertions/updates.
   * 
   * No changes are committed until the call to commitBatchUpdate returns.
   * A call to abortBatchUpdate will abandon the entire batch.
   *
   * @param row name of row to be updated
   * @return lockid to be used in subsequent put, delete and commit calls
   */
  public long startBatchUpdate(final Text row) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().startUpdate(row);
  }
  
  /** 
   * Abort a batch mutation
   * @param lockid lock id returned by startBatchUpdate
   */
  public void abortBatch(final long lockid) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().abort(lockid);
  }
  
  /** 
   * Finalize a batch mutation
   *
   * @param lockid lock id returned by startBatchUpdate
   * @throws IOException
   */
  public void commitBatch(final long lockid) throws IOException {
    commitBatch(lockid, System.currentTimeMillis());
  }

  /** 
   * Finalize a batch mutation
   *
   * @param lockid lock id returned by startBatchUpdate
   * @param timestamp time to associate with all the changes
   * @throws IOException
   */
  public void commitBatch(final long lockid, final long timestamp)
  throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().commit(lockid, timestamp);
  }
  
  /** 
   * Start an atomic row insertion/update.  No changes are committed until the 
   * call to commit() returns. A call to abort() will abandon any updates in progress.
   *
   * Callers to this method are given a lease for each unique lockid; before the
   * lease expires, either abort() or commit() must be called. If it is not 
   * called, the system will automatically call abort() on the client's behalf.
   *
   * The client can gain extra time with a call to renewLease().
   * Start an atomic row insertion or update
   * 
   * @param row Name of row to start update against.
   * @return Row lockid.
   */
  public long startUpdate(final Text row) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    return this.table.get().startUpdate(row);
  }
  
  /** 
   * Change a value for the specified column.
   * Runs {@link #abort(long)} if exception thrown.
   *
   * @param lockid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column
   */
  public void put(long lockid, Text column, byte val[]) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().put(lockid, column, val);
  }
  
  /** 
   * Delete the value for a column
   *
   * @param lockid              - lock id returned from startUpdate
   * @param column              - name of column whose value is to be deleted
   */
  public void delete(long lockid, Text column) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().delete(lockid, column);
  }
  
  /** 
   * Abort a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   */
  public void abort(long lockid) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().abort(lockid);
  }
  
  /** 
   * Finalize a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   * @throws IOException
   */
  public void commit(long lockid) throws IOException {
    commit(lockid, System.currentTimeMillis());
  }

  /** 
   * Finalize a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   * @param timestamp           - time to associate with the change
   * @throws IOException
   */
  public void commit(long lockid, long timestamp) throws IOException {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
    this.table.get().commit(lockid, timestamp);
  }
  
  /**
   * Renew lease on update
   * 
   * @param lockid              - lock id returned from startUpdate
   */
  public void renewLease(@SuppressWarnings("unused") long lockid) {
    if(this.table.get() == null) {
      throw new IllegalStateException("Must open table first");
    }
  }

  private void printUsage() {
    printUsage(null);
  }
  
  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
        " [--master=host:port] <command> <args>");
    System.err.println("Options:");
    System.err.println(" master       Specify host and port of HBase " +
        "cluster master. If not present,");
    System.err.println("              address is read from configuration.");
    System.err.println("Commands:");
    System.err.println(" shutdown     Shutdown the HBase cluster.");
    System.err.println(" createTable  Create named table.");
    System.err.println(" deleteTable  Delete named table.");
    System.err.println(" listTables   List all tables.");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() + " shutdown");
    System.err.println(" % java " + this.getClass().getName() +
        " createTable webcrawl contents: anchors: 10");
  }
  
  private void printCreateTableUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
      " [options] createTable <name> <colfamily1> ... <max_versions>");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() +
      " createTable testtable column_x column_y column_z 3");
  }
  
  private void printDeleteTableUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
      " [options] deleteTable <name>");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() +
      " deleteTable testtable");
  }
  
  /**
   * Process command-line args.
   * @param args - command arguments
   * @return 0 if successful -1 otherwise
   */
  public int doCommandLine(final String args[]) {
    // TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).    
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage();
          errCode = 0;
          break;
        }
        
        final String masterArgKey = "--master=";
        if (cmd.startsWith(masterArgKey)) {
          this.conf.set(MASTER_ADDRESS, cmd.substring(masterArgKey.length()));
          continue;
        }
       
        if (cmd.equals("shutdown")) {
          shutdown();
          errCode = 0;
          break;
        }
        
        if (cmd.equals("listTables")) {
          HTableDescriptor [] tables = listTables();
          for (int ii = 0; ii < tables.length; ii++) {
            System.out.println(tables[ii].getName());
          }
          errCode = 0;
          break;
        }
        
        if (cmd.equals("createTable")) {
          if (i + 2 > args.length) {
            printCreateTableUsage("Error: Supply a table name," +
              " at least one column family, and maximum versions");
            errCode = 1;
            break;
          }
          HTableDescriptor desc = new HTableDescriptor(args[i + 1]);
          boolean addedFamily = false;
          for (int ii = i + 2; ii < (args.length - 1); ii++) {
            desc.addFamily(new HColumnDescriptor(args[ii]));
            addedFamily = true;
          }
          if (!addedFamily) {
            throw new IllegalArgumentException("Must supply at least one " +
              "column family");
          }
          createTable(desc);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("deleteTable")) {
          if (i + 1 > args.length) {
            printDeleteTableUsage("Error: Must supply a table name");
            errCode = 1;
            break;
          }
          deleteTable(new Text(args[i + 1]));
          errCode = 0;
          break;
        }
        
        printUsage();
        break;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    
    return errCode;
  }    

  /**
   * @return the configuration for this client
   */
  protected Configuration getConf(){
    return conf;
  }
  
  /**
   * Main program
   * @param args
   */
  public static void main(final String args[]) {
    Configuration c = new HBaseConfiguration();
    int errCode = (new HClient(c)).doCommandLine(args);
    System.exit(errCode);
  }

}
