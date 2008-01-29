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

package org.apache.hadoop.hbase.thrift;

import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.NotFound;
import org.apache.hadoop.hbase.thrift.generated.RegionDescriptor;
import org.apache.hadoop.hbase.thrift.generated.ScanEntry;
import org.apache.hadoop.io.Text;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;

/**
 * ThriftServer - this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServer {
  
  /**
   * The HBaseHandler is a glue object that connects Thrift RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseHandler implements Hbase.Iface {
    protected HBaseConfiguration conf = new HBaseConfiguration();
    protected HBaseAdmin admin = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected HashMap<Integer, HScannerInterface> scannerMap = null;
    
    /**
     * Creates and returns an HTable instance from a given table name.
     * 
     * @param tableName
     *          name of table
     * @return HTable object
     * @throws IOException
     * @throws IOException
     */
    protected HTable getTable(final byte[] tableName) throws IOError,
        IOException {
      return new HTable(this.conf, getText(tableName));
    }
    
    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     * 
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addScanner(HScannerInterface scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      return id;
    }
    
    /**
     * Returns the scanner associated with the specified ID.
     * 
     * @param id
     * @return a HScannerInterface, or null if ID was invalid.
     */
    protected synchronized HScannerInterface getScanner(int id) {
      return scannerMap.get(id);
    }
    
    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     * 
     * @param id
     * @return a HScannerInterface, or null if ID was invalid.
     */
    protected synchronized HScannerInterface removeScanner(int id) {
      return scannerMap.remove(id);
    }
    
    /**
     * Constructs an HBaseHandler object.
     * 
     * @throws MasterNotRunningException
     */
    HBaseHandler() throws MasterNotRunningException {
      conf = new HBaseConfiguration();
      admin = new HBaseAdmin(conf);
      scannerMap = new HashMap<Integer, HScannerInterface>();
    }
    
    /**
     * Converts a byte array to a Text object after validating the UTF-8
     * encoding.
     * 
     * @param buf
     *          UTF-8 encoded bytes
     * @return Text object
     * @throws IllegalArgument
     */
    Text getText(byte[] buf) throws IOError {
      try {
        Text.validateUTF8(buf);
      } catch (MalformedInputException e) {
        throw new IOError("invalid UTF-8 encoding in row or column name");
      }
      return new Text(buf);
    }
    
    //
    // The Thrift Hbase.Iface interface is implemented below.
    // Documentation for the methods and datastructures is the Hbase.thrift file
    // used to generate the interface.
    //
    
    public ArrayList<byte[]> getTableNames() throws IOError {
      LOG.debug("getTableNames");
      try {
        HTableDescriptor[] tables = this.admin.listTables();
        ArrayList<byte[]> list = new ArrayList<byte[]>(tables.length);
        for (int i = 0; i < tables.length; i++) {
          list.add(tables[i].getName().toString().getBytes());
        }
        return list;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public ArrayList<RegionDescriptor> getTableRegions(byte[] tableName)
        throws IOError {
      LOG.debug("getTableRegions: " + new String(tableName));
      try {
        HTable table = getTable(tableName);
        Text[] startKeys = table.getStartKeys();
        ArrayList<RegionDescriptor> regions = new ArrayList<RegionDescriptor>();
        for (int i = 0; i < startKeys.length; i++) {
          RegionDescriptor region = new RegionDescriptor();
          region.startKey = startKeys[i].toString().getBytes();
          regions.add(region);
        }
        return regions;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public byte[] get(byte[] tableName, byte[] row, byte[] column)
        throws NotFound, IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("get: table=" + new String(tableName) + ", row="
            + new String(row) + ", col=" + new String(column));
      }
      try {
        HTable table = getTable(tableName);
        byte[] value = table.get(getText(row), getText(column));
        if (value == null) {
          throw new NotFound();
        }
        return value;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public ArrayList<byte[]> getVer(byte[] tableName, byte[] row,
        byte[] column, int numVersions) throws IOError, NotFound {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getVer: table=" + new String(tableName) + ", row="
            + new String(row) + ", col=" + new String(column) + ", numVers="
            + numVersions);
      }
      try {
        HTable table = getTable(tableName);
        byte[][] values = table.get(getText(row), getText(column), numVersions);
        if (values == null) {
          throw new NotFound();
        }
        return new ArrayList<byte[]>(Arrays.asList(values));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public ArrayList<byte[]> getVerTs(byte[] tableName, byte[] row,
        byte[] column, long timestamp, int numVersions) throws IOError,
        NotFound {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getVerTs: table=" + new String(tableName) + ", row="
            + new String(row) + ", col=" + new String(column) + ", ts="
            + timestamp + ", numVers=" + numVersions);
      }
      try {
        HTable table = getTable(tableName);
        byte[][] values = table.get(getText(row), getText(column), timestamp,
            numVersions);
        if (values == null) {
          throw new NotFound();
        }
        return new ArrayList<byte[]>(Arrays.asList(values));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public AbstractMap<byte[], byte[]> getRow(byte[] tableName, byte[] row)
        throws IOError {
      return getRowTs(tableName, row, HConstants.LATEST_TIMESTAMP);
    }
    
    public AbstractMap<byte[], byte[]> getRowTs(byte[] tableName, byte[] row,
        long timestamp) throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getRowTs: table=" + new String(tableName) + ", row="
            + new String(row) + ", ts=" + timestamp);
      }
      try {
        HTable table = getTable(tableName);
        SortedMap<Text, byte[]> values = table.getRow(getText(row), timestamp);
        // copy the map from type <Text, byte[]> to <byte[], byte[]>
        HashMap<byte[], byte[]> returnValues = new HashMap<byte[], byte[]>();
        for (Entry<Text, byte[]> e : values.entrySet()) {
          returnValues.put(e.getKey().getBytes(), e.getValue());
        }
        return returnValues;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public void put(byte[] tableName, byte[] row, byte[] column, byte[] value)
        throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("put: table=" + new String(tableName) + ", row="
            + new String(row) + ", col=" + new String(column)
            + ", value.length=" + value.length);
      }
      try {
        HTable table = getTable(tableName);
        long lockid = table.startUpdate(getText(row));
        table.put(lockid, getText(column), value);
        table.commit(lockid);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public void deleteAll(byte[] tableName, byte[] row, byte[] column)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP);
    }
    
    public void deleteAllTs(byte[] tableName, byte[] row, byte[] column,
        long timestamp) throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteAllTs: table=" + new String(tableName) + ", row="
            + new String(row) + ", col=" + new String(column) + ", ts="
            + timestamp);
      }
      try {
        HTable table = getTable(tableName);
        table.deleteAll(getText(row), getText(column), timestamp);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public void deleteAllRow(byte[] tableName, byte[] row) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP);
    }
    
    public void deleteAllRowTs(byte[] tableName, byte[] row, long timestamp)
        throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteAllRowTs: table=" + new String(tableName) + ", row="
            + new String(row) + ", ts=" + timestamp);
      }
      try {
        HTable table = getTable(tableName);
        table.deleteAll(getText(row), timestamp);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public void createTable(byte[] tableName,
        ArrayList<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
      if (LOG.isDebugEnabled()) {
        LOG.debug("createTable: table=" + new String(tableName));
      }
      try {
        Text tableStr = getText(tableName);
        if (admin.tableExists(tableStr)) {
          throw new AlreadyExists("table name already in use");
        }
        HTableDescriptor desc = new HTableDescriptor(tableStr.toString());
        for (ColumnDescriptor col : columnFamilies) {
          HColumnDescriptor colDesc = ThriftUtilities.colDescFromThrift(col);
          desc.addFamily(colDesc);
          if (LOG.isDebugEnabled()) {
            LOG.debug("createTable:     col=" + colDesc.getName());
          }
        }
        admin.createTable(desc);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }
    
    public void deleteTable(byte[] tableName) throws IOError, NotFound {
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + new String(tableName));
      }
      try {
        Text tableStr = getText(tableName);
        if (!admin.tableExists(tableStr)) {
          throw new NotFound();
        }
        admin.deleteTable(tableStr);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public void mutateRow(byte[] tableName, byte[] row,
        ArrayList<Mutation> mutations) throws IOError {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP);
    }
    
    public void mutateRowTs(byte[] tableName, byte[] row,
        ArrayList<Mutation> mutations, long timestamp) throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("mutateRowTs: table=" + new String(tableName) + ", row="
            + new String(row) + ", ts=" + timestamp + " mutations="
            + mutations.size());
        for (Mutation m : mutations) {
          if (m.isDelete) {
            LOG.debug("mutateRowTs:    : delete - " + getText(m.column));
          } else {
            LOG.debug("mutateRowTs:    : put - " + getText(m.column) + " => "
                + m.value);
          }
        }
      }
      
      Long lockid = null;
      HTable table = null;
      
      try {
        table = getTable(tableName);
        lockid = table.startUpdate(getText(row));
        for (Mutation m : mutations) {
          if (m.isDelete) {
            table.delete(lockid, getText(m.column));
          } else {
            table.put(lockid, getText(m.column), m.value);
          }
        }
        table.commit(lockid, timestamp);
      } catch (IOException e) {
        if (lockid != null) {
          table.abort(lockid);
        }
        throw new IOError(e.getMessage());
      }
    }
    
    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      HScannerInterface scanner = getScanner(id);
      if (scanner == null) {
        throw new IllegalArgument("scanner ID is invalid");
      }
      try {
        scanner.close();
        removeScanner(id);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public ScanEntry scannerGet(int id) throws IllegalArgument, NotFound,
        IOError {
      LOG.debug("scannerGet: id=" + id);
      HScannerInterface scanner = getScanner(id);
      if (scanner == null) {
        throw new IllegalArgument("scanner ID is invalid");
      }
      
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      
      try {
        if (scanner.next(key, results) == false) {
          throw new NotFound("end of scanner reached");
        }
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
      
      ScanEntry retval = new ScanEntry();
      retval.row = key.getRow().getBytes();
      retval.columns = new HashMap<byte[], byte[]>(results.size());
      
      for (SortedMap.Entry<Text, byte[]> e : results.entrySet()) {
        retval.columns.put(e.getKey().getBytes(), e.getValue());
      }
      return retval;
    }
    
    public int scannerOpen(byte[] tableName, byte[] startRow,
        ArrayList<byte[]> columns) throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scannerOpen: table=" + getText(tableName) + ", start="
            + getText(startRow) + ", columns=" + columns.toString());
      }
      try {
        HTable table = getTable(tableName);
        Text[] columnsText = new Text[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
          columnsText[i] = getText(columns.get(i));
        }
        HScannerInterface scanner = table.obtainScanner(columnsText,
            getText(startRow));
        return addScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public int scannerOpenWithStop(byte[] tableName, byte[] startRow,
        byte[] stopRow, ArrayList<byte[]> columns) throws IOError, TException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scannerOpen: table=" + getText(tableName) + ", start="
            + getText(startRow) + ", stop=" + getText(stopRow) + ", columns="
            + columns.toString());
      }
      try {
        HTable table = getTable(tableName);
        Text[] columnsText = new Text[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
          columnsText[i] = getText(columns.get(i));
        }
        HScannerInterface scanner = table.obtainScanner(columnsText,
            getText(startRow), getText(stopRow));
        return addScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public int scannerOpenTs(byte[] tableName, byte[] startRow,
        ArrayList<byte[]> columns, long timestamp) throws IOError, TException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scannerOpen: table=" + getText(tableName) + ", start="
            + getText(startRow) + ", columns=" + columns.toString()
            + ", timestamp=" + timestamp);
      }
      try {
        HTable table = getTable(tableName);
        Text[] columnsText = new Text[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
          columnsText[i] = getText(columns.get(i));
        }
        HScannerInterface scanner = table.obtainScanner(columnsText,
            getText(startRow), timestamp);
        return addScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public int scannerOpenWithStopTs(byte[] tableName, byte[] startRow,
        byte[] stopRow, ArrayList<byte[]> columns, long timestamp)
        throws IOError, TException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scannerOpen: table=" + getText(tableName) + ", start="
            + getText(startRow) + ", stop=" + getText(stopRow) + ", columns="
            + columns.toString() + ", timestamp=" + timestamp);
      }
      try {
        HTable table = getTable(tableName);
        Text[] columnsText = new Text[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
          columnsText[i] = getText(columns.get(i));
        }
        HScannerInterface scanner = table.obtainScanner(columnsText,
            getText(startRow), getText(stopRow), timestamp);
        return addScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
    
    public AbstractMap<byte[], ColumnDescriptor> getColumnDescriptors(
        byte[] tableName) throws IOError, TException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getColumnDescriptors: table=" + new String(tableName));
      }
      try {
        HashMap<byte[], ColumnDescriptor> columns = new HashMap<byte[], ColumnDescriptor>();
        
        HTable table = getTable(tableName);
        HTableDescriptor desc = table.getMetadata();
        
        for (Entry<Text, HColumnDescriptor> e : desc.families().entrySet()) {
          ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e.getValue());
          columns.put(col.name, col);
        }
        return columns;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
  }
  
  //
  // Main program and support routines
  //
  
  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }
  
  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.out.println("Usage: java org.apache.hadoop.hbase.thrift.ThriftServer " +
      "--help | [--port=PORT] start");
    System.out.println("Arguments:");
    System.out.println(" start Start thrift server");
    System.out.println(" stop  Stop thrift server");
    System.out.println("Options:");
    System.out.println(" port  Port to listen on. Default: 9090");
    // System.out.println(" bind  Address to bind on. Default: 0.0.0.0.");
    System.out.println(" help  Print this message and exit");
    System.exit(0);
  }

  /*
   * Start up the REST servlet in standalone mode.
   * @param args
   */
  protected static void doMain(final String [] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }

    int port = 9090;
    // String bindAddress = "0.0.0.0";

    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
//    final String addressArgKey = "--bind=";
    final String portArgKey = "--port=";
    for (String cmd: args) {
//      if (cmd.startsWith(addressArgKey)) {
//        bindAddress = cmd.substring(addressArgKey.length());
//        continue;
//      } else 
      if (cmd.startsWith(portArgKey)) {
        port = Integer.parseInt(cmd.substring(portArgKey.length()));
        continue;
      } else if (cmd.equals("--help") || cmd.equals("-h")) {
        printUsageAndExit();
      } else if (cmd.equals("start")) {
        continue;
      } else if (cmd.equals("stop")) {
        printUsageAndExit("To shutdown the thrift server run " +
          "bin/hbase-daemon.sh stop thrift or send a kill signal to " +
          "the thrift server pid");
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
    Log LOG = LogFactory.getLog("ThriftServer");
    LOG.info("starting HBase Thrift server on port " +
      Integer.toString(port));
    HBaseHandler handler = new HBaseHandler();
    Hbase.Processor processor = new Hbase.Processor(handler);
    TServerTransport serverTransport = new TServerSocket(port);
    TProtocolFactory protFactory = new TBinaryProtocol.Factory(true, true);
    TServer server = new TThreadPoolServer(processor, serverTransport,
      protFactory);
    server.serve();
  }
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String [] args) throws Exception {
    doMain(args);
  }
}
