/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

/*******************************************************************************
 * HClient manages a connection to a single HRegionServer.
 ******************************************************************************/
public class HClient implements HConstants {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  private static final Text[] META_COLUMNS = {
    COLUMN_FAMILY
  };
  
  private static final Text EMPTY_START_ROW = new Text();
  
  private boolean closed;
  private long clientTimeout;
  private int numTimeouts;
  private int numRetries;
  private HMasterInterface master;
  private final Configuration conf;
  
  private class TableInfo {
    public HRegionInfo regionInfo;
    public HServerAddress serverAddress;

    TableInfo(HRegionInfo regionInfo, HServerAddress serverAddress) {
      this.regionInfo = regionInfo;
      this.serverAddress = serverAddress;
    }
  }
  
  // Map tableName -> (Map startRow -> (HRegionInfo, HServerAddress)
  
  private TreeMap<Text, TreeMap<Text, TableInfo>> tablesToServers;
  
  // For the "current" table: Map startRow -> (HRegionInfo, HServerAddress)
  
  private TreeMap<Text, TableInfo> tableServers;
  
  // Known region HServerAddress.toString() -> HRegionInterface
  
  private TreeMap<String, HRegionInterface> servers;
  
  // For row mutation operations

  private Text currentRegion;
  private HRegionInterface currentServer;
  private Random rand;
  private long clientid;

  /** Creates a new HClient */
  public HClient(Configuration conf) {
    this.closed = false;
    this.conf = conf;

    this.clientTimeout = conf.getLong("hbase.client.timeout.length", 30 * 1000);
    this.numTimeouts = conf.getInt("hbase.client.timeout.number", 5);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    
    this.master = null;
    this.tablesToServers = new TreeMap<Text, TreeMap<Text, TableInfo>>();
    this.tableServers = null;
    this.servers = new TreeMap<String, HRegionInterface>();
    
    // For row mutation operations
    
    this.currentRegion = null;
    this.currentServer = null;
    this.rand = new Random();
  }
  
  /**
   * Check client is open.
   */
  private void checkOpen() {
    if (this.closed) {
      throw new IllegalStateException("client is not open");
    }
  }
  
  /**
   * Find the address of the master and connect to it
   */
  private void checkMaster() {
    if (this.master != null) {
      return;
    }
    for(int tries = 0; this.master == null && tries < numRetries; tries++) {
      HServerAddress masterLocation =
        new HServerAddress(this.conf.get(MASTER_ADDRESS));
      
      try {
        HMasterInterface tryMaster =
          (HMasterInterface)RPC.getProxy(HMasterInterface.class, 
              HMasterInterface.versionID, masterLocation.getInetSocketAddress(),
              this.conf);
        
        if(tryMaster.isMasterRunning()) {
          this.master = tryMaster;
          break;
        }
      } catch(IOException e) {
        if(tries == numRetries - 1) {
          // This was our last chance - don't bother sleeping
          break;
        }
      }
      
      // We either cannot connect to the master or it is not running.
      // Sleep and retry
      
      try {
        Thread.sleep(this.clientTimeout);
        
      } catch(InterruptedException e) {
      }
    }
    if(this.master == null) {
      throw new IllegalStateException("Master is not running");
    }
  }

  public synchronized void createTable(HTableDescriptor desc) throws IOException {
    if(desc.getName().equals(ROOT_TABLE_NAME)
        || desc.getName().equals(META_TABLE_NAME)) {
      
      throw new IllegalArgumentException(desc.getName().toString()
          + " is a reserved table name");
    }
    checkOpen();
    checkMaster();
    locateRootRegion();
    this.master.createTable(desc);
  }

  public synchronized void deleteTable(Text tableName) throws IOException {
    checkOpen();
    checkMaster();
    locateRootRegion();
    this.master.deleteTable(tableName);
  }
  
  public synchronized void shutdown() throws IOException {
    checkOpen();
    checkMaster();
    this.master.shutdown();
  }
  
  public synchronized void openTable(Text tableName) throws IOException {
    if(tableName == null || tableName.getLength() == 0) {
      throw new IllegalArgumentException("table name cannot be null or zero length");
    }
    checkOpen();
    this.tableServers = tablesToServers.get(tableName);
    if(this.tableServers == null ) {            // We don't know where the table is
      findTableInMeta(tableName);               // Load the information from meta
    }
  }

  private void findTableInMeta(Text tableName) throws IOException {
    TreeMap<Text, TableInfo> metaServers =
      this.tablesToServers.get(META_TABLE_NAME);
    
    if(metaServers == null) {                   // Don't know where the meta is
      loadMetaFromRoot(tableName);
      if(tableName.equals(META_TABLE_NAME) || tableName.equals(ROOT_TABLE_NAME)) {
        // All we really wanted was the meta or root table
        return;
      }
      metaServers = this.tablesToServers.get(META_TABLE_NAME);
    }

    this.tableServers = new TreeMap<Text, TableInfo>();
    for(int tries = 0;
        this.tableServers.size() == 0 && tries < this.numRetries;
        tries++) {
      
      Text firstMetaRegion = null;
      if(metaServers.containsKey(tableName)) {
        firstMetaRegion = tableName;
        
      } else {
        firstMetaRegion = metaServers.headMap(tableName).lastKey();
      }
      for(Iterator<TableInfo> i
          = metaServers.tailMap(firstMetaRegion).values().iterator();
          i.hasNext(); ) {
      
        TableInfo t = i.next();
      
        scanOneMetaRegion(t, tableName);
      }
      if(this.tableServers.size() == 0) {
        // Table not assigned. Sleep and try again

        if(LOG.isDebugEnabled()) {
          LOG.debug("Sleeping. Table " + tableName
              + " not currently being served.");
        }
        try {
          Thread.sleep(this.clientTimeout);
          
        } catch(InterruptedException e) {
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("Wake. Retry finding table " + tableName);
        }
      }
    }
    if(this.tableServers.size() == 0) {
      throw new IOException("failed to scan " + META_TABLE_NAME + " after "
          + this.numRetries + " retries");
    }
    this.tablesToServers.put(tableName, this.tableServers);
  }

  /*
   * Load the meta table from the root table.
   */
  private void loadMetaFromRoot(Text tableName) throws IOException {
    locateRootRegion();
    if(tableName.equals(ROOT_TABLE_NAME)) {   // All we really wanted was the root
      return;
    }
    scanRoot();
  }
  
  /*
   * Repeatedly try to find the root region by asking the HMaster for where it
   * could be.
   */
  private void locateRootRegion() throws IOException {
    checkMaster();
    
    HServerAddress rootRegionLocation = null;
    for(int tries = 0; rootRegionLocation == null && tries < numRetries; tries++){
      int localTimeouts = 0;
      while(rootRegionLocation == null && localTimeouts < numTimeouts) {
        rootRegionLocation = master.findRootRegion();

        if(rootRegionLocation == null) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleeping. Waiting for root region.");
            }
            Thread.sleep(this.clientTimeout);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Wake. Retry finding root region.");
            }
          } catch(InterruptedException iex) {
          }
          localTimeouts++;
        }
      }
      if(rootRegionLocation == null) {
        throw new IOException("Timed out trying to locate root region");
      }
      
      // Verify that this server still serves the root region
      
      HRegionInterface rootRegion = getHRegionConnection(rootRegionLocation);

      if(rootRegion.getRegionInfo(HGlobals.rootRegionInfo.regionName) != null) {
        this.tableServers = new TreeMap<Text, TableInfo>();
        this.tableServers.put(EMPTY_START_ROW,
            new TableInfo(HGlobals.rootRegionInfo, rootRegionLocation));
        
        this.tablesToServers.put(ROOT_TABLE_NAME, this.tableServers);
        break;
      }
      rootRegionLocation = null;
    }
    
    if (rootRegionLocation == null) {
      this.closed = true;
      throw new IOException("unable to locate root region server");
    }
  }

  /*
   * Scans the root region to find all the meta regions
   */
  private void scanRoot() throws IOException {
    this.tableServers = new TreeMap<Text, TableInfo>();
    TableInfo t = this.tablesToServers.get(ROOT_TABLE_NAME).get(EMPTY_START_ROW);
    for(int tries = 0;
        scanOneMetaRegion(t, META_TABLE_NAME) == 0 && tries < this.numRetries;
        tries++) {
      
      // The table is not yet being served. Sleep and retry.
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("Sleeping. Table " + META_TABLE_NAME
            + " not currently being served.");
      }
      try {
        Thread.sleep(this.clientTimeout);
        
      } catch(InterruptedException e) {
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Retry finding table " + META_TABLE_NAME);
      }
    }
    if(this.tableServers.size() == 0) {
      throw new IOException("failed to scan " + ROOT_TABLE_NAME + " after "
          + this.numRetries + " retries");
    }
    this.tablesToServers.put(META_TABLE_NAME, this.tableServers);
  }

  /*
   * Scans a single meta region
   * @param t the table we're going to scan
   * @param tableName the name of the table we're looking for
   * @return returns the number of servers that are serving the table
   */
  private int scanOneMetaRegion(TableInfo t, Text tableName)
      throws IOException {
    
    HRegionInterface server = getHRegionConnection(t.serverAddress);
    int servers = 0;
    long scannerId = -1L;
    try {
      scannerId =
        server.openScanner(t.regionInfo.regionName, META_COLUMNS, tableName);
      
      DataInputBuffer inbuf = new DataInputBuffer();
      while(true) {
        HRegionInfo regionInfo = null;
        String serverAddress = null;
        HStoreKey key = new HStoreKey();
        LabelledData[] values = server.next(scannerId, key);
        if(values.length == 0) {
          if(servers == 0) {
            // If we didn't find any servers then the table does not exist
            
            throw new NoSuchElementException("table '" + tableName
                + "' does not exist");
          }
          
          // We found at least one server for the table and now we're done.
          
          break;
        }

        byte[] bytes = null;
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        for(int i = 0; i < values.length; i++) {
          bytes = new byte[values[i].getData().getSize()];
          System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
          results.put(values[i].getLabel(), bytes);
        }
        regionInfo = new HRegionInfo();
        bytes = results.get(COL_REGIONINFO);
        inbuf.reset(bytes, bytes.length);
        regionInfo.readFields(inbuf);

        if(!regionInfo.tableDesc.getName().equals(tableName)) {
          // We're done
          break;
        }

        bytes = results.get(COL_SERVER);
        if(bytes == null || bytes.length == 0) {
          // We need to rescan because the table we want is unassigned.
          
          if(LOG.isDebugEnabled()) {
            LOG.debug("no server address for " + regionInfo.toString());
          }
          servers = 0;
          this.tableServers.clear();
          break;
        }
        servers += 1;
        serverAddress = new String(bytes, UTF8_ENCODING);

        this.tableServers.put(regionInfo.startKey, 
            new TableInfo(regionInfo, new HServerAddress(serverAddress)));
      }
      return servers;
      
    } finally {
      if(scannerId != -1L) {
        server.close(scannerId);
      }
    }
  }

  synchronized HRegionInterface getHRegionConnection(HServerAddress regionServer)
      throws IOException {

      // See if we already have a connection

    HRegionInterface server = this.servers.get(regionServer.toString());
    
    if(server == null) {                                // Get a connection
      
      server = (HRegionInterface)RPC.waitForProxy(HRegionInterface.class, 
          HRegionInterface.versionID, regionServer.getInetSocketAddress(),
          this.conf);
      
      this.servers.put(regionServer.toString(), server);
    }
    return server;
  }

  /** Close the connection */
  public synchronized void close() throws IOException {
    if(! this.closed) {
      RPC.stopClient();
      this.closed = true;
    }
  }

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   */
  public synchronized HTableDescriptor[] listTables()
  throws IOException {
    TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    
    TreeMap<Text, TableInfo> metaTables =
      this.tablesToServers.get(META_TABLE_NAME);
    
    if(metaTables == null) {
      // Meta is not loaded yet so go do that
      loadMetaFromRoot(META_TABLE_NAME);
      metaTables = tablesToServers.get(META_TABLE_NAME);
    }

    for (TableInfo t: metaTables.values()) {
      HRegionInterface server = getHRegionConnection(t.serverAddress);
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(t.regionInfo.regionName,
            META_COLUMNS, EMPTY_START_ROW);
        
        HStoreKey key = new HStoreKey();
        DataInputBuffer inbuf = new DataInputBuffer();
        while(true) {
          LabelledData[] values = server.next(scannerId, key);
          if(values.length == 0) {
            break;
          }
          for(int i = 0; i < values.length; i++) {
            if(values[i].getLabel().equals(COL_REGIONINFO)) {
              byte[] bytes = values[i].getData().get();
              inbuf.reset(bytes, bytes.length);
              HRegionInfo info = new HRegionInfo();
              info.readFields(inbuf);

              // Only examine the rows where the startKey is zero length   
              if(info.startKey.getLength() == 0) {
                uniqueTables.add(info.tableDesc);
              }
            }
          }
        }
      } finally {
        if(scannerId != -1L) {
          server.close(scannerId);
        }
      }
    }
    return (HTableDescriptor[])uniqueTables.
      toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  private synchronized TableInfo getTableInfo(Text row) {
    if(row == null || row.getLength() == 0) {
      throw new IllegalArgumentException("row key cannot be null or zero length");
    }
    if(this.tableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    
    // Only one server will have the row we are looking for
    
    Text serverKey = null;
    if(this.tableServers.containsKey(row)) {
      serverKey = row;
      
    } else {
      serverKey = this.tableServers.headMap(row).lastKey();
    }
    return this.tableServers.get(serverKey);
  }
  
  /** Get a single value for the specified row and column */
  public byte[] get(Text row, Text column) throws IOException {
    TableInfo info = getTableInfo(row);
    return getHRegionConnection(info.serverAddress).get(
        info.regionInfo.regionName, row, column).get();
  }
 
  /** Get the specified number of versions of the specified row and column */
  public byte[][] get(Text row, Text column, int numVersions) throws IOException {
    TableInfo info = getTableInfo(row);
    BytesWritable[] values = getHRegionConnection(info.serverAddress).get(
        info.regionInfo.regionName, row, column, numVersions);
    
    ArrayList<byte[]> bytes = new ArrayList<byte[]>();
    for(int i = 0 ; i < values.length; i++) {
      bytes.add(values[i].get());
    }
    return bytes.toArray(new byte[values.length][]);
  }
  
  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   */
  public byte[][] get(Text row, Text column, long timestamp, int numVersions) throws IOException {
    TableInfo info = getTableInfo(row);
    BytesWritable[] values = getHRegionConnection(info.serverAddress).get(
        info.regionInfo.regionName, row, column, timestamp, numVersions);
    
    ArrayList<byte[]> bytes = new ArrayList<byte[]>();
    for(int i = 0 ; i < values.length; i++) {
      bytes.add(values[i].get());
    }
    return bytes.toArray(new byte[values.length][]);
  }

  /** Get all the data for the specified row */
  public LabelledData[] getRow(Text row) throws IOException {
    TableInfo info = getTableInfo(row);
    return getHRegionConnection(info.serverAddress).getRow(
        info.regionInfo.regionName, row);
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns, Text startRow) throws IOException {
    if(this.tableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    return new ClientScanner(columns, startRow);
  }

  /** Start an atomic row insertion or update */
  public long startUpdate(Text row) throws IOException {
    TableInfo info = getTableInfo(row);
    long lockid;
    try {
      this.currentServer = getHRegionConnection(info.serverAddress);
      this.currentRegion = info.regionInfo.regionName;
      this.clientid = rand.nextLong();
      lockid = currentServer.startUpdate(this.currentRegion, this.clientid, row);
      
    } catch(IOException e) {
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
    return lockid;
  }
  
  /** Change a value for the specified column */
  public void put(long lockid, Text column, byte val[]) throws IOException {
    try {
      this.currentServer.put(this.currentRegion, this.clientid, lockid, column,
          new BytesWritable(val));
      
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
        
      } catch(IOException e2) {
      }
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Delete the value for a column */
  public void delete(long lockid, Text column) throws IOException {
    try {
      this.currentServer.delete(this.currentRegion, this.clientid, lockid, column);
      
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
        
      } catch(IOException e2) {
      }
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Abort a row mutation */
  public void abort(long lockid) throws IOException {
    try {
      this.currentServer.abort(this.currentRegion, this.clientid, lockid);
    } catch(IOException e) {
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Finalize a row mutation */
  public void commit(long lockid) throws IOException {
    try {
      this.currentServer.commit(this.currentRegion, this.clientid, lockid);
      
    } finally {
      this.currentServer = null;
      this.currentRegion = null;
    }
  }
  
  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  private class ClientScanner implements HScannerInterface {
    private Text[] columns;
    private Text startRow;
    private boolean closed;
    private TableInfo[] regions;
    private int currentRegion;
    private HRegionInterface server;
    private long scannerId;
    
    public ClientScanner(Text[] columns, Text startRow) throws IOException {
      this.columns = columns;
      this.startRow = startRow;
      this.closed = false;
      
      Text firstServer = null;
      if(this.startRow == null || this.startRow.getLength() == 0) {
        firstServer = tableServers.firstKey();
        
      } else if(tableServers.containsKey(startRow)) {
        firstServer = startRow;
        
      } else {
        firstServer = tableServers.headMap(startRow).lastKey();
      }
      Collection<TableInfo> info = tableServers.tailMap(firstServer).values();
      
      this.regions = info.toArray(new TableInfo[info.size()]);
      this.currentRegion = -1;
      this.server = null;
      this.scannerId = -1L;
      nextScanner();
    }
    
    /*
     * Gets a scanner for the next region.
     * Returns false if there are no more scanners.
     */
    private boolean nextScanner() throws IOException {
      if(this.scannerId != -1L) {
        this.server.close(this.scannerId);
        this.scannerId = -1L;
      }
      this.currentRegion += 1;
      if(this.currentRegion == this.regions.length) {
        close();
        return false;
      }
      try {
        this.server = getHRegionConnection(this.regions[currentRegion].serverAddress);
        this.scannerId = this.server.openScanner(
            this.regions[currentRegion].regionInfo.regionName, this.columns,
            this.startRow);
        
      } catch(IOException e) {
        close();
        throw e;
      }
      return true;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HScannerInterface#next(org.apache.hadoop.hbase.HStoreKey, java.util.TreeMap)
     */
    public boolean next(HStoreKey key, TreeMap<Text, byte[]> results) throws IOException {
      if(this.closed) {
        return false;
      }
      LabelledData[] values = null;
      do {
        values = this.server.next(this.scannerId, key);
      } while(values.length == 0 && nextScanner());

      for(int i = 0; i < values.length; i++) {
        byte[] bytes = new byte[values[i].getData().getSize()];
        System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
        results.put(values[i].getLabel(), bytes);
      }
      return values.length != 0;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HScannerInterface#close()
     */
    public void close() throws IOException {
      if(this.scannerId != -1L) {
        this.server.close(this.scannerId);
      }
      this.server = null;
      this.closed = true;
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
    System.err.println(" createTable  Takes table name, column families, " +
      "and maximum versions.");
    System.err.println(" deleteTable  Takes a table name.");
    System.err.println(" iistTables   List all tables.");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() + " shutdown");
    System.err.println(" % java " + this.getClass().getName() +
        " createTable webcrawl contents: anchors: 10");
  }
  
  int doCommandLine(final String args[]) {
    // Process command-line args. TODO: Better cmd-line processing
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
          if (i + 3 > args.length) {
            throw new IllegalArgumentException("Must supply a table name " +
              ", at least one column family and maximum number of versions");
          }
          int maxVersions = (Integer.parseInt(args[args.length - 1]));
          HTableDescriptor desc =
            new HTableDescriptor(args[i + 1], maxVersions);
          boolean addedFamily = false;
          for (int ii = i + 2; ii < (args.length - 1); ii++) {
            desc.addFamily(new Text(args[ii]));
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
            throw new IllegalArgumentException("Must supply a table name");
          }
          deleteTable(new Text(args[i + 1]));
          errCode = 0;
          break;
        }
        
        printUsage();
        break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    return errCode;
  }
  
  public static void main(final String args[]) {
    Configuration c = new HBaseConfiguration();
    int errCode = (new HClient(c)).doCommandLine(args);
    System.exit(errCode);
  }
}